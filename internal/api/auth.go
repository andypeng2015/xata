package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	authv1 "xata/gen/proto/auth/v1"
	"xata/internal/api/key"
	"xata/internal/grpc"
	"xata/internal/token"
	"xata/services/auth/api/spec"

	"github.com/labstack/echo/v4"
)

type UserClaimsKey struct{}

// attr key to use for api key
const APIKeyO11yK = "xata.apikey"

// attr key to use for user id
const UserIDO11yK = "xata.user"

// attr key to use for organization id
const OrganizationO11yK = "xata.organization"

// AuthMiddleware is a middleware that checks for valid credentials in the Authorization header
// User claims are stored in the echo context and can be retrieved with `GetUserClaims`

func AuthMiddleware(conn *grpc.ClientConnection) echo.MiddlewareFunc {
	client := authv1.NewAuthServiceClient(conn)

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			tokenStr, err := tokenFromHeader(c)
			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized, "no token found")
			}

			validateResponse, err := client.ValidateAccess(c.Request().Context(), &authv1.ValidateAccessRequest{
				Token:          tokenStr,
				Method:         c.Request().Method,
				Path:           c.Path(),
				Scopes:         spec.GetScopes(c.Request().Method, c.Path()),
				OrganizationId: c.Param("organizationID"),
				ProjectId:      c.Param("projectID"),
				BranchId:       c.Param("branchID"),
			})
			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized, "invalid token")
			}

			// If access is not allowed for this endpoint, deny
			if !validateResponse.Allow {
				return echo.NewHTTPError(http.StatusForbidden, "access denied")
			}

			// token is valid
			claims := token.Claims{
				ID:            validateResponse.UserId,
				KeyID:         validateResponse.ApiKeyId,
				Email:         validateResponse.UserEmail,
				Scopes:        validateResponse.GetScopes(),
				Organizations: mapOrganizations(validateResponse.GetOrganizations()),
				Projects:      validateResponse.GetProjects(),
				Branches:      validateResponse.GetBranches(),
			}

			// store claims in context
			ctx := context.WithValue(c.Request().Context(), UserClaimsKey{}, &claims)
			c.SetRequest(c.Request().WithContext(ctx))

			// Set API key in o11y attributes if available
			k := key.Key(tokenStr)
			if k.IsValid() {
				c.Set(APIKeyO11yK, k.Obfuscate(key.DefaultObfuscateCharsCount))
			}

			return next(c)
		}
	}
}

func mapOrganizations(src map[string]*authv1.Organization) map[string]token.Organization {
	dst := make(map[string]token.Organization, len(src))
	for id, o := range src {
		dst[id] = token.Organization{
			ID:        o.Id,
			Status:    o.Status,
			CreatedAt: o.CreatedAt.AsTime(),
			UsageTier: o.GetUsageTier(),
		}
	}
	return dst
}

func tokenFromHeader(c echo.Context) (string, error) {
	authScheme := "Bearer"
	auth := c.Request().Header.Get(echo.HeaderAuthorization)
	l := len(authScheme)
	if len(auth) > l+1 && strings.EqualFold(strings.ToLower(auth[:l]), strings.ToLower(authScheme)) {
		return auth[l+1:], nil
	}
	return "", errors.New("no token found")
}

// GetUserClaims from echo context, returns nil if not found
func GetUserClaims(c echo.Context) *token.Claims {
	return GetUserClaimsFromContext(c.Request().Context())
}

func GetUserClaimsFromContext(ctx context.Context) *token.Claims {
	if claims, ok := ctx.Value(UserClaimsKey{}).(*token.Claims); ok {
		return claims
	}
	return nil
}
