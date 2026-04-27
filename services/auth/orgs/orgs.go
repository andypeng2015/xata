package orgs

import (
	"context"
	"fmt"
	"time"

	projectsv1 "xata/gen/proto/projects/v1"
	"xata/services/auth/api"
	"xata/services/auth/api/spec"
	"xata/services/auth/keycloak"

	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog/log"
)

//go:generate go run github.com/vektra/mockery/v3 --output orgsmock --outpkg orgsmock --with-expecter --name Organizations

// Max number of retries for updating organization status in projects service
const projectsMaxRetries = uint64(5)

type Organizations interface {
	UpdateOrganization(ctx context.Context, organizationID string, request UpdateOrganizationOptions) (*spec.Organization, error)
}
type orgsService struct {
	realm          string
	kcRest         keycloak.KeyCloak
	projectsClient projectsv1.ProjectsServiceClient
	newBackoff     func() backoff.BackOff
}

func NewOrganizations(realm string, kcRest keycloak.KeyCloak, projectsClient projectsv1.ProjectsServiceClient) Organizations {
	return &orgsService{
		realm:          realm,
		kcRest:         kcRest,
		projectsClient: projectsClient,
		newBackoff: func() backoff.BackOff {
			return backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(30*time.Second), backoff.WithMaxInterval(3*time.Second))
		},
	}
}

type BillingStatus string

const (
	BillingStatusOk                BillingStatus = "ok"
	BillingStatusNoPaymentMethod   BillingStatus = "no_payment_method"
	BillingStatusInvoiceOverdue    BillingStatus = "invoice_overdue"
	BillingStatusUnknown           BillingStatus = "unknown"
	BillingStatusDeletionRequested BillingStatus = "deletion_requested"
)

type UpdateOrganizationOptions struct {
	DisabledByAdmin       *bool
	DisabledByAdminReason *string
	BillingStatus         *BillingStatus
	BillingReason         *string
	UsageTier             *spec.OrganizationStatusUsageTier
}

func ValueOrDefault[T any](p *T, fallback T) T {
	if p != nil {
		return *p
	}
	return fallback
}

func (o *orgsService) UpdateOrganization(
	ctx context.Context,
	organizationID string,
	req UpdateOrganizationOptions,
) (*spec.Organization, error) {
	// Ensure the organization exists
	organization, err := o.kcRest.GetOrganization(ctx, o.realm, organizationID)
	if err != nil {
		return nil, api.ErrorNoOrganizationAccess{OrganizationID: organizationID}
	}

	// Desired state
	shouldDisableByAdmin := ValueOrDefault(req.DisabledByAdmin, organization.Status.DisabledByAdmin)
	shouldBillingStatus := ValueOrDefault(req.BillingStatus, BillingStatus(organization.Status.BillingStatus))

	update := keycloak.OrganizationUpdate{}

	if organization.Status.DisabledByAdmin != shouldDisableByAdmin {
		update.DisabledByAdmin = &shouldDisableByAdmin
		if req.DisabledByAdminReason != nil {
			update.AdminReason = req.DisabledByAdminReason
		}
	}

	if string(organization.Status.BillingStatus) != string(shouldBillingStatus) {
		update.BillingStatus = new(string(shouldBillingStatus))
		if req.BillingReason != nil {
			update.BillingReason = req.BillingReason
		}
	}

	tierChanged := req.UsageTier != nil && *req.UsageTier != organization.Status.UsageTier
	if tierChanged {
		tier := string(*req.UsageTier)
		update.UsageTier = &tier
	}

	// Only update if flags actually changed (reasons alone do nothing)
	if update.DisabledByAdmin != nil || update.BillingStatus != nil || update.UsageTier != nil {
		targetStatus := orgStatus(string(shouldBillingStatus), shouldDisableByAdmin)

		if targetStatus == string(spec.Enabled) && organization.Status.Status == spec.Disabled {
			update.ResourcesCleanedAt = new("")
		}

		org, err := o.kcRest.UpdateOrganization(ctx, o.realm, organizationID, update)
		if err != nil {
			return nil, fmt.Errorf("update organization in Keycloak: %w", err)
		}

		// Then trigger the change in the projects service, but only if general status changed
		if targetStatus != string(organization.Status.Status) {
			_, err := o.retryWithBackoff(ctx, o.projectsClient, &projectsv1.UpdateOrganizationStatusRequest{
				OrganizationId: organizationID,
				Disabled:       targetStatus != string(spec.Enabled),
			})
			if err != nil {
				return nil, fmt.Errorf("propagate organization status to projects: %w", err)
			}
		} else {
			log.Ctx(ctx).Debug().Msg("general organization status hasn't changed; skipping projects service update")
		}

		return &org, nil
	}

	// Nothing changed at all (flags unchanged, reasons ignored)
	return &organization, nil
}

// retryWithBackoff provides retry logic with backoff for updating organization status in the projects service
func (o *orgsService) retryWithBackoff(ctx context.Context, client projectsv1.ProjectsServiceClient, req *projectsv1.UpdateOrganizationStatusRequest) (*projectsv1.UpdateOrganizationStatusResponse, error) {
	var result *projectsv1.UpdateOrganizationStatusResponse
	op := func() error {
		var err error
		result, err = client.UpdateOrganizationStatus(ctx, req)
		return err
	}

	bo := backoff.WithMaxRetries(o.newBackoff(), projectsMaxRetries)
	err := backoff.RetryNotify(op, backoff.WithContext(bo, ctx),
		func(err error, d time.Duration) {
			log.Ctx(ctx).Warn().Err(err).Dur("retry_in", d).Msg("update organization status in projects service; retrying")
		})

	return result, err
}

func orgStatus(billingStatus string, disabledByAdmin bool) string {
	if billingStatus == string(spec.Ok) && !disabledByAdmin {
		return "enabled"
	}
	return "disabled"
}
