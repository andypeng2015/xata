package customerio

type EmailMessageData interface {
	TriggerName() string
}

type DummyTestEmailV1 struct {
	UserName         string `json:"user_name"`
	OrganizationName string `json:"organization_name"`
}

func (DummyTestEmailV1) TriggerName() string {
	return "dummy_test_email_v1"
}

type BillingTrialEndedNoPaymentMethodV3 struct {
	OrganizationID         string  `json:"organization_id"`
	OrganizationName       string  `json:"organization_name"`
	ActiveCredits          float64 `json:"active_credits"`
	ActiveCreditsFormatted string  `json:"active_credits_formatted"`
	FreeTrialDays          int     `json:"free_trial_days"`
	LifetimeTotalCredits   float64 `json:"lifetime_total_credits"`
	BillingSettingsURL     string  `json:"billing_settings_url"`
	ConsoleURL             string  `json:"console_url"`
}

func (BillingTrialEndedNoPaymentMethodV3) TriggerName() string {
	return "billing_trial_ended_no_payment_method_v3"
}

type BillingTrialCreditDroppedNoPaymentMethodV3 struct {
	OrganizationID         string  `json:"organization_id"`
	OrganizationName       string  `json:"organization_name"`
	ActiveCredits          float64 `json:"active_credits"`
	ActiveCreditsFormatted string  `json:"active_credits_formatted"`
	DropThreshold          float64 `json:"drop_threshold"`
	DropThresholdFormatted string  `json:"drop_threshold_formatted"`
	FreeTrialDays          int     `json:"free_trial_days"`
	TrialExpiryDate        *int64  `json:"trial_expiry_date"`
	TrialExpiryDays        *int    `json:"trial_expiry_days"`
	BillingSettingsURL     string  `json:"billing_settings_url"`
	ConsoleURL             string  `json:"console_url"`
}

func (BillingTrialCreditDroppedNoPaymentMethodV3) TriggerName() string {
	return "billing_trial_credit_dropped_no_payment_method_v3"
}
