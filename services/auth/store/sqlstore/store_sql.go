package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"xata/internal/api/key"
	"xata/internal/idgen"
	"xata/internal/o11y"
	"xata/internal/pgroll"
	"xata/services/auth/store"
)

//go:embed migrations/*.json
var migrationsFS embed.FS

// check if sqlAuthStore implements the AuthStore interface
var _ store.AuthStore = (*sqlAuthStore)(nil)

const (
	// Unique constraint name for API keys
	UniqueConstraintKeyName = "unique_api_key_name"
)

type sqlAuthStore struct {
	config Config
	sql    *sql.DB
	pgroll *pgroll.PGRoll
}

func NewSQLAuthStore(ctx context.Context, cfg Config) (*sqlAuthStore, error) {
	// set search path to the latest known version
	pgroll, err := pgroll.FromEmbeddedFS(&migrationsFS)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgroll: %w", err)
	}

	// connect to the database (with the latest schema version)
	latest := pgroll.LatestVersionSchema(ctx)
	db, err := sql.Open("postgres", cfg.ConnectionString()+"&search_path="+latest)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	return &sqlAuthStore{
		sql:    db,
		config: cfg,
		pgroll: pgroll,
	}, nil
}

// Setup runs DB migrations for the store
func (s *sqlAuthStore) Setup(ctx context.Context) error {
	// TODO move this to its own package (+ CLI tool?)
	logger := o11y.Ctx(ctx).Logger()
	logger.Info().Msg("Running DB migrations")

	err := s.pgroll.ApplyMigrations(ctx, s.config.ConnectionString())
	if err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}
	return nil
}

func (s *sqlAuthStore) Close(ctx context.Context) error {
	return s.sql.Close()
}

// scanAPIKey scans a single row into an APIKey
func scanAPIKey(row *sql.Row) (*store.APIKey, error) {
	if err := row.Err(); err != nil {
		return nil, fmt.Errorf("query execution error: %w", err)
	}

	apiKey, err := scanAPIKeyFrom(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &store.ErrAPIKeyNotFound{ID: ""}
		}
		return nil, err
	}

	return apiKey, nil
}

// scanAPIKeyFrom scans using a generic scanner to avoid duplicate logic.
func scanAPIKeyFrom(scanner interface{ Scan(dest ...any) error }) (*store.APIKey, error) {
	var (
		apiKey           store.APIKey
		nullExpiry       sql.NullTime
		nullLastUsed     sql.NullTime
		nullCreatedBy    sql.NullString
		nullCreatedByKey sql.NullString
	)

	if err := scanner.Scan(
		&apiKey.ID,
		&apiKey.Name,
		&apiKey.KeyHash,
		&apiKey.KeyPreview,
		&apiKey.TargetType,
		&apiKey.TargetID,
		&nullExpiry,
		&apiKey.CreatedAt,
		&nullLastUsed,
		pq.Array(&apiKey.Scopes),
		pq.Array(&apiKey.Projects),
		pq.Array(&apiKey.Branches),
		&nullCreatedBy,
		&nullCreatedByKey,
	); err != nil {
		return nil, fmt.Errorf("failed to scan API key: %w", err)
	}

	if nullExpiry.Valid {
		apiKey.Expiry = &nullExpiry.Time
	}

	if nullLastUsed.Valid {
		apiKey.LastUsed = &nullLastUsed.Time
	}

	if nullCreatedBy.Valid {
		apiKey.CreatedBy = &nullCreatedBy.String
	}

	if nullCreatedByKey.Valid {
		apiKey.CreatedByKey = &nullCreatedByKey.String
	}

	return &apiKey, nil
}

// scanAPIKeys scans rows into APIKey structs
func scanAPIKeys(rows *sql.Rows) ([]store.APIKey, error) {
	var apiKeys []store.APIKey

	// Check for any errors that occurred during query execution before iterating
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query execution error: %w", err)
	}

	for rows.Next() {
		apiKey, err := scanAPIKeyFrom(rows)
		if err != nil {
			return nil, err
		}
		apiKeys = append(apiKeys, *apiKey)
	}

	// Check for any errors encountered during iteration
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return apiKeys, nil
}

// ValidateAPIKey validates the provided API Key.
func (s *sqlAuthStore) ValidateAPIKey(ctx context.Context, apiKey key.Key) (*store.APIKey, error) {
	{
		// Use HMAC lookup for fast validation
		hashedKey := apiKey.HashKey(s.config.HMACSecret)

		row := s.sql.QueryRowContext(ctx, `
		SELECT id, name, key_hash, key_preview, target_type, target_id, expiry, created_at, last_used,
		       scopes, projects, branches, created_by, created_by_key
		FROM api_keys
		WHERE key_hash = $1
	`, hashedKey)

		apiKey, err := scanAPIKey(row)
		if err != nil {
			return nil, &store.ErrInvalidAPIKey{}
		}

		// Check if the API key has expired
		if apiKey.Expiry != nil && apiKey.Expiry.Before(time.Now()) {
			return nil, &store.ErrInvalidAPIKey{}
		}

		// Update last_used timestamp in the background
		go func(apiKeyID string, parentCtx context.Context) {
			updateCtx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
			defer cancel()

			_, err := s.sql.ExecContext(updateCtx, "UPDATE api_keys SET last_used = NOW() WHERE id = $1", apiKeyID)
			if err != nil {
				// Non-critical error, just log it
				logger := o11y.Ctx(parentCtx).Logger()
				logger.Error().Err(err).Str("api_key_id", apiKeyID).Msg("Failed to update last_used timestamp")
			}
		}(apiKey.ID, ctx)

		return apiKey, nil
	}
}

// DeleteAPIKeys deletes API keys by their IDs for a specific target type and target ID.
func (s *sqlAuthStore) DeleteAPIKeys(ctx context.Context, targetType store.KeyTargetType, targetID string, keyIDs []string) error {
	if len(keyIDs) == 0 {
		return nil
	}

	_, err := s.sql.ExecContext(ctx, `
		DELETE FROM api_keys
		WHERE id = ANY($1) AND target_type = $2 AND target_id = $3
	`, pq.Array(keyIDs), targetType, targetID)
	if err != nil {
		return err
	}

	return nil
}

// GetAPIKey retrieves an API key by its ID.
func (s *sqlAuthStore) GetAPIKey(ctx context.Context, id string) (*store.APIKey, error) {
	row := s.sql.QueryRowContext(ctx, `
		SELECT id, name, key_hash, key_preview, target_type, target_id, expiry, created_at, last_used,
		       scopes, projects, branches, created_by, created_by_key
		FROM api_keys
		WHERE id = $1
	`, id)

	apiKey, err := scanAPIKey(row)
	if err != nil {
		var notFound *store.ErrAPIKeyNotFound
		if errors.As(err, &notFound) {
			return nil, &store.ErrAPIKeyNotFound{ID: id}
		}
		return nil, fmt.Errorf("get API key: %w", err)
	}
	return apiKey, nil
}

// ListAPIKeys retrieves all API keys for a specific target type and target ID.
func (s *sqlAuthStore) ListAPIKeys(ctx context.Context, targetType store.KeyTargetType, targetID string) ([]store.APIKey, error) {
	rows, err := s.sql.QueryContext(ctx, `
		SELECT id, name, key_hash, key_preview, target_type, target_id, expiry, created_at, last_used,
		       scopes, projects, branches, created_by, created_by_key
		FROM api_keys
		WHERE target_type = $1 AND target_id = $2
	`, targetType, targetID)
	if err != nil {
		return nil, fmt.Errorf("failed to query API keys: %w", err)
	}
	defer rows.Close()

	return scanAPIKeys(rows)
}

// generateAPIKey creates a new API key based on the target type.
func (s *sqlAuthStore) generateAPIKey(targetType store.KeyTargetType) (key.Key, error) {
	switch targetType {
	case store.KeyTargetOrganization:
		return key.NewOrganizationKey()
	case store.KeyTargetUser:
		return key.NewUserKey()
	default:
		return "", &store.ErrUnsupportedTargetType{TargetType: string(targetType)}
	}
}

// CreateAPIKey creates a new API key for a specific target type and target ID.
func (s *sqlAuthStore) CreateAPIKey(ctx context.Context, targetType store.KeyTargetType, targetID string, keyInfo *store.APIKeyCreate) (key.Key, *store.APIKey, error) {
	// Validate expiry time if provided
	if keyInfo.Expiry != nil && keyInfo.Expiry.Before(time.Now()) {
		return "", nil, &store.ErrAPIKeyExpiresInPast{Expiry: keyInfo.Expiry}
	}

	// Validate scopes
	if len(keyInfo.Scopes) > store.MaxScopesPerAPIKey {
		return "", nil, &store.ErrAPIKeyScopesLimitReached{Limit: store.MaxScopesPerAPIKey}
	}
	if len(keyInfo.Scopes) == 0 {
		keyInfo.Scopes = []string{"*"}
	}

	// Validate projects
	if len(keyInfo.Projects) > store.MaxProjectsPerAPIKey {
		return "", nil, &store.ErrAPIKeyProjectsLimitReached{Limit: store.MaxProjectsPerAPIKey}
	}
	if len(keyInfo.Projects) == 0 {
		keyInfo.Projects = []string{"*"}
	}

	// Validate branches
	if len(keyInfo.Branches) > store.MaxBranchesPerAPIKey {
		return "", nil, &store.ErrAPIKeyBranchesLimitReached{Limit: store.MaxBranchesPerAPIKey}
	}
	if len(keyInfo.Branches) == 0 {
		keyInfo.Branches = []string{"*"}
	}

	rawKey, err := s.generateAPIKey(targetType)
	if err != nil {
		return "", nil, err
	}

	hashedKey := rawKey.HashKey(s.config.HMACSecret)

	var expiry sql.NullTime
	if keyInfo.Expiry != nil {
		expiry.Valid = true
		expiry.Time = *keyInfo.Expiry
	}

	tx, err := s.sql.BeginTx(ctx, nil)
	if err != nil {
		return "", nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Enforce maximum number of API keys per target within the transaction
	var count int
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM api_keys WHERE target_type = $1 AND target_id = $2`, targetType, targetID).Scan(&count)
	if err != nil {
		return "", nil, fmt.Errorf("failed to count API keys: %w", err)
	}
	if count >= store.MaxAPIKeysPerTarget {
		return "", nil, &store.ErrAPIKeyLimitReached{Limit: store.MaxAPIKeysPerTarget}
	}

	var createdBy, createdByKey *string
	if keyInfo.CreatedBy != nil && *keyInfo.CreatedBy != "" {
		createdBy = keyInfo.CreatedBy
	} else {
		createdBy = nil
	}
	if keyInfo.CreatedByKey != nil && *keyInfo.CreatedByKey != "" {
		createdByKey = keyInfo.CreatedByKey
	} else {
		createdByKey = nil
	}

	row := tx.QueryRowContext(ctx, `
		INSERT INTO api_keys (id, name, key_hash, key_preview, target_type, target_id, expiry, created_at, last_used,
							  scopes, projects, branches, created_by, created_by_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NULL, $8, $9, $10, $11, $12)
		RETURNING id, name, key_hash, key_preview, target_type, target_id, expiry, created_at, last_used,
				  scopes, projects, branches, created_by, created_by_key
	`, idgen.Generate(), keyInfo.Name, hashedKey, rawKey.Obfuscate(key.DefaultObfuscateCharsCount),
		targetType, targetID, expiry,
		pq.Array(keyInfo.Scopes), pq.Array(keyInfo.Projects), pq.Array(keyInfo.Branches), createdBy, createdByKey,
	)

	apiKey, err := scanAPIKey(row)
	if err != nil {
		// API key already exists
		if IsConstraintError(err, UniqueConstraintKeyName) {
			return "", nil, &store.ErrAPIKeyAlreadyExists{Name: keyInfo.Name}
		}

		return "", nil, err
	}

	if err := tx.Commit(); err != nil {
		return "", nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return rawKey, apiKey, nil
}

// IsConstraintError checks if a given constraint was not met
func IsConstraintError(err error, constraint string) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pqErr.Code == "23505" && pqErr.Constraint == constraint
	}
	return false
}

func decodeLimits[K ~string](raw []byte) (map[K]any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var m map[K]any
	if err := d.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

// GetOrgLimits returns the stored limit overrides for an organization.
func (s *sqlAuthStore) GetOrgLimits(ctx context.Context, orgID string) (map[store.OrgLimitKey]any, error) {
	var raw []byte
	err := s.sql.QueryRowContext(ctx, `
		SELECT limits
		FROM organization_limits
		WHERE organization_id = $1
	`, orgID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return map[store.OrgLimitKey]any{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query org limits: %w", err)
	}
	limits, err := decodeLimits[store.OrgLimitKey](raw)
	if err != nil {
		return nil, fmt.Errorf("unmarshal org limits: %w", err)
	}
	return limits, nil
}

// SetOrgLimit upserts an override for a single organization limit.
func (s *sqlAuthStore) SetOrgLimit(ctx context.Context, orgID string, key store.OrgLimitKey, value any) error {
	if !key.IsValid() {
		return fmt.Errorf("unknown org limit key %q", key)
	}
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal limit value: %w", err)
	}
	_, err = s.sql.ExecContext(ctx, `
		INSERT INTO organization_limits (organization_id, limits)
		VALUES ($1, jsonb_build_object($2::text, $3::jsonb))
		ON CONFLICT (organization_id) DO UPDATE
		SET limits = organization_limits.limits || jsonb_build_object($2::text, $3::jsonb)
	`, orgID, key, valueJSON)
	if err != nil {
		return fmt.Errorf("set org limit: %w", err)
	}
	return nil
}

// DeleteOrgLimit removes an override for a single organization limit.
func (s *sqlAuthStore) DeleteOrgLimit(ctx context.Context, orgID string, key store.OrgLimitKey) error {
	_, err := s.sql.ExecContext(ctx, `
		UPDATE organization_limits
		SET limits = limits - $2::text
		WHERE organization_id = $1
	`, orgID, key)
	if err != nil {
		return fmt.Errorf("delete org limit: %w", err)
	}
	return nil
}
