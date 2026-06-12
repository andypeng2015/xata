package billing

import (
	"context"
	"time"
)

//go:generate go run github.com/vektra/mockery/v3 --output billingmock --outpkg billingmock --with-expecter --name Client

const (
	TrialCreditExpiryDays      = 15
	CreditStatusActive         = "active"
	CreditStatusPendingPayment = "pending_payment"
)

type Subscription struct {
	ID string
}

type Credit struct {
	ID                    string
	Amount                float64
	MaximumInitialBalance float64
	EffectiveDate         time.Time
	ExpiryDate            time.Time
	Status                string
}

func (c *Credit) IsExpired() bool {
	// The orb client uses a zero time to indicate no expiry
	return !c.ExpiryDate.IsZero() && time.Now().After(c.ExpiryDate)
}

type PaymentMethodCard struct {
	Brand       string
	Last4       string
	ExpiryMonth int
	ExpiryYear  int
}

type PaymentMethod struct {
	Card PaymentMethodCard
}

type Customer struct {
	ID                    string
	CustomerID            string
	CustomerExternalID    string
	Name                  string
	Email                 string
	PaymentProviderID     string
	Subscriptions         []Subscription
	Credits               []Credit
	DefaultPaymentMethod  *PaymentMethod
	HasValidPaymentMethod bool
}

func (c *Customer) CurrentActiveCredit() float64 {
	var credit float64
	for _, credits := range c.Credits {
		if !credits.IsExpired() && credits.Amount > 0 {
			credit += float64(credits.Amount)
		}
	}

	return credit
}

func (c *Customer) TotalLifetimeCredits() float64 {
	var total float64
	for _, credit := range c.Credits {
		total += credit.MaximumInitialBalance
	}
	return total
}

type InvoiceAutoCollection struct {
	NumAttempts   int
	NextAttemptAt *time.Time
}

type Invoice struct {
	ID             string
	InvoiceNumber  string
	AmountDue      float64
	Total          float64
	Currency       string
	Status         string
	InvoiceDate    time.Time
	DueDate        time.Time
	IssuedAt       time.Time
	PaidAt         time.Time
	InvoicePDF     *string
	AutoCollection InvoiceAutoCollection
}

type InvoiceListOptions struct {
	Cursor string
	Limit  int
}

type InvoicesPage struct {
	Data       []Invoice
	HasMore    bool
	NextCursor *string
}

type UpcomingInvoice struct {
	CreatedAt        time.Time
	AmountDue        float64
	Currency         string
	TargetDate       time.Time
	Subtotal         float64
	Total            float64
	HostedInvoiceURL *string
}

type OrbCustomerMetadata struct {
	Marketplace string
}

func (m OrbCustomerMetadata) Values() map[string]string {
	if m.Marketplace == "" {
		return nil
	}
	return map[string]string{"marketplace": m.Marketplace}
}

type StripeCustomer struct {
	ID             string
	OrganizationID string
}

type CheckoutSession struct {
	URL string
}

type PaymentMethodSession struct {
	URL string
}

type StripePaymentMethodCard struct {
	ID    string
	Last4 string
}

type Client interface {
	// CreateCustomer creates a new customer in the billing system
	CreateCustomer(ctx context.Context, name, email, externalCustomerID string, organizationsCount int, metadata OrbCustomerMetadata) error
	CustomerExists(ctx context.Context, externalCustomerID string) (bool, error)
	// FetchCustomer retrieves a customer record from the billing system.
	// The customerID parameter should be the Orb internal customer ID (not the external customer ID).
	FetchCustomer(ctx context.Context, customerID string) (*Customer, error)
	// FetchCustomerByExternalID retrieves a customer record by the external customer ID.
	FetchCustomerByExternalID(ctx context.Context, externalCustomerID string) (*Customer, error)
	// FetchBillingCustomerWithDefaultPaymentMethod retrieves a customer with their default payment method details.
	FetchBillingCustomerWithDefaultPaymentMethod(ctx context.Context, externalCustomerID string) (*Customer, error)
	UpdateOrbCustomerEmail(ctx context.Context, externalCustomerID, email string) (*Customer, error)
	ListInvoices(ctx context.Context, externalCustomerID string, opts InvoiceListOptions) (*InvoicesPage, error)
	// FetchUpcomingInvoice fetches the next invoice for the customer's active subscription.
	// Customers are expected to have at most one active subscription.
	FetchUpcomingInvoice(ctx context.Context, externalCustomerID string) (*UpcomingInvoice, error)
	// ListCustomersCreatedAfter retrieves customer records created at or after the given time.
	ListCustomersCreatedAfter(ctx context.Context, createdAfter time.Time) ([]*Customer, error)
	// FetchStripeCustomer retrieves a Stripe customer by their Stripe customer ID.
	FetchStripeCustomer(ctx context.Context, stripeCustomerID string) (*StripeCustomer, error)
	// FetchPaymentIntentPaymentMethodID retrieves the payment method attached to a Stripe payment intent.
	FetchPaymentIntentPaymentMethodID(ctx context.Context, paymentIntentID string) (string, error)
	// FetchSetupIntentPaymentMethodID retrieves the payment method attached to a Stripe setup intent.
	FetchSetupIntentPaymentMethodID(ctx context.Context, setupIntentID string) (string, error)
	// FetchStripePaymentMethodCard retrieves Stripe card details for a payment method.
	FetchStripePaymentMethodCard(ctx context.Context, paymentMethodID string) (*StripePaymentMethodCard, error)
	// FetchInvoice retrieves an orb invoice. invoiceID is an Orb internal invoice id
	FetchInvoice(ctx context.Context, invoiceID string) (*Invoice, error)
	// EnsureDefaultPaymentMethod sets paymentMethodID as the Stripe customer's default payment method
	// if it is not already set.
	EnsureDefaultPaymentMethod(ctx context.Context, stripeCustomerID, paymentMethodID string) error
	// SetOrbCustomerStripeChargeProvider configures the Orb customer to charge the Stripe customer.
	SetOrbCustomerStripeChargeProvider(ctx context.Context, externalCustomerID, stripeCustomerID string) error
	// RefundPaymentIntent creates a Stripe refund for a payment intent.
	RefundPaymentIntent(ctx context.Context, paymentIntentID string, metadata map[string]string, idempotencyKey string) error
	// HasValidDefaultPaymentMethod returns true if the Stripe customer has a valid default payment method.
	HasValidDefaultPaymentMethod(ctx context.Context, stripeCustomerID string) (bool, error)
	// VoidInvoice voids an invoice in the billing system.
	VoidInvoice(ctx context.Context, invoiceID string) error
	// CountPendingInvoices returns the number of issued (unpaid) invoices that have had at least one payment attempt for the given external customer ID.
	CountPendingInvoices(ctx context.Context, externalCustomerID string) (int, error)
	// HasOutstandingInvoices returns true if the customer has any issued invoices or draft invoices with a non-zero total.
	HasOutstandingInvoices(ctx context.Context, externalCustomerID string) (bool, error)
	// FinalizeSubscription cancels any active Orb subscriptions for the customer immediately.
	FinalizeSubscription(ctx context.Context, externalCustomerID string) error
	// DeletePaymentMethod detaches the customer's default Stripe payment method, if any.
	DeletePaymentMethod(ctx context.Context, externalCustomerID string) error
	CreateStripeCheckoutSession(ctx context.Context, externalCustomerID string) (*CheckoutSession, error)
	CreateStripePaymentMethodSession(ctx context.Context, externalCustomerID string) (*PaymentMethodSession, error)
}

type NoopBilling struct{}

func (n *NoopBilling) CreateCustomer(ctx context.Context, name, email, externalCustomerID string, organizationsCount int, metadata OrbCustomerMetadata) error {
	return nil
}

func (n *NoopBilling) CustomerExists(ctx context.Context, customerID string) (bool, error) {
	return false, nil
}

func (n *NoopBilling) FetchCustomer(ctx context.Context, externalCustomerID string) (*Customer, error) {
	return nil, nil
}

func (n *NoopBilling) FetchCustomerByExternalID(_ context.Context, _ string) (*Customer, error) {
	return nil, nil
}

func (n *NoopBilling) FetchBillingCustomerWithDefaultPaymentMethod(_ context.Context, _ string) (*Customer, error) {
	return nil, nil
}

func (n *NoopBilling) UpdateOrbCustomerEmail(_ context.Context, _, _ string) (*Customer, error) {
	return nil, nil
}

func (n *NoopBilling) ListInvoices(_ context.Context, _ string, _ InvoiceListOptions) (*InvoicesPage, error) {
	return nil, nil
}

func (n *NoopBilling) FetchUpcomingInvoice(_ context.Context, _ string) (*UpcomingInvoice, error) {
	return nil, nil
}

func (n *NoopBilling) ListCustomersCreatedAfter(_ context.Context, _ time.Time) ([]*Customer, error) {
	return nil, nil
}

func (n *NoopBilling) FetchInvoice(_ context.Context, _ string) (*Invoice, error) {
	return nil, nil
}

func (n *NoopBilling) FetchStripeCustomer(_ context.Context, _ string) (*StripeCustomer, error) {
	return nil, nil
}

func (n *NoopBilling) FetchPaymentIntentPaymentMethodID(_ context.Context, _ string) (string, error) {
	return "", nil
}

func (n *NoopBilling) FetchSetupIntentPaymentMethodID(_ context.Context, _ string) (string, error) {
	return "", nil
}

func (n *NoopBilling) FetchStripePaymentMethodCard(_ context.Context, _ string) (*StripePaymentMethodCard, error) {
	return nil, nil
}

func (n *NoopBilling) EnsureDefaultPaymentMethod(_ context.Context, _, _ string) error {
	return nil
}

func (n *NoopBilling) SetOrbCustomerStripeChargeProvider(_ context.Context, _, _ string) error {
	return nil
}

func (n *NoopBilling) RefundPaymentIntent(_ context.Context, _ string, _ map[string]string, _ string) error {
	return nil
}

func (n *NoopBilling) HasValidDefaultPaymentMethod(_ context.Context, _ string) (bool, error) {
	return false, nil
}

func (n *NoopBilling) VoidInvoice(_ context.Context, _ string) error {
	return nil
}

func (n *NoopBilling) CountPendingInvoices(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (n *NoopBilling) HasOutstandingInvoices(_ context.Context, _ string) (bool, error) {
	return false, nil
}

func (n *NoopBilling) FinalizeSubscription(_ context.Context, _ string) error {
	return nil
}

func (n *NoopBilling) DeletePaymentMethod(_ context.Context, _ string) error {
	return nil
}

func (n *NoopBilling) CreateStripeCheckoutSession(_ context.Context, _ string) (*CheckoutSession, error) {
	return nil, nil
}

func (n *NoopBilling) CreateStripePaymentMethodSession(_ context.Context, _ string) (*PaymentMethodSession, error) {
	return nil, nil
}
