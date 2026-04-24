package clusters

import (
	"slices"
	"strings"

	"xata/internal/extensions"
	"xata/internal/postgresversions"
)

// MandatoryPostgresParameters are the Postgres parameters that must always be
// set on a Cluster - the user is not permitted to omit or override them.
func MandatoryPostgresParameters(image string) map[string]string {
	return map[string]string{
		"lc_messages": "C.utf8",
		"lc_monetary": "C.utf8",
		"lc_numeric":  "C.utf8",
		"lc_time":     "C.utf8",

		"xatautils.privileged_role": "xata",

		"xatautils.privileged_extensions": privilegedExtensions(image),

		"xatautils.extension_custom_scripts_path": "/etc/xatautils/extensions",

		"xatautils.reserved_memberships": "xata_superuser, pg_read_server_files, pg_write_server_files, pg_execute_server_program",

		"xatautils.reserved_roles": "cnpg_pooler_pgbouncer",

		"xatautils.privileged_role_allowed_configs": "auth_delay.*, auto_explain.*, log_lock_waits, log_min_duration_statement, log_min_messages, log_replication_commands, log_statement, log_temp_files, pg_net.batch_size, pg_net.ttl, pg_stat_statements.*, pgaudit.log, pgaudit.log_catalog, pgaudit.log_client, pgaudit.log_level, pgaudit.log_relation, pgaudit.log_rows, pgaudit.log_statement, pgaudit.log_statement_once, pgaudit.role, pgrst.*, plan_filter.*, safeupdate.enabled, session_replication_role, track_io_timing, wal_compression",
	}
}

// privilegedExtensions returns a comma-separated, alphabetically sorted list of
// extension names for the given image.
func privilegedExtensions(image string) string {
	shortImage := postgresversions.ShortImageName(image)
	exts := extensions.GetExtensions(shortImage)
	extNames := make([]string, len(exts))
	for i, ext := range exts {
		extNames[i] = ext.Name
	}
	slices.Sort(extNames)
	return strings.Join(extNames, ", ")
}
