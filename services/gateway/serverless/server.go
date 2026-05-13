package serverless

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"time"

	capi "xata/internal/api"
	"xata/internal/o11y"
	"xata/services/gateway/metrics"
	"xata/services/gateway/serverless/spec"
	"xata/services/gateway/session"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	proxyproto "github.com/pires/go-proxyproto"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	ListenAddress string
	TLSCert       *tls.Certificate
}

type Server struct {
	echo   *echo.Echo
	config Config
}

type handler struct {
	resolver session.BranchResolver
	dialer   *session.ClusterDialer
	tracer   trace.Tracer
	metrics  *metrics.GatewayMetrics
	ipFilter session.IPFilter
}

func NewServer(
	o *o11y.O,
	resolver session.BranchResolver,
	dialer *session.ClusterDialer,
	gwMetrics *metrics.GatewayMetrics,
	ipFilter session.IPFilter,
	config Config,
) (*Server, error) {
	tracer := o.Tracer("gateway.http")

	h := &handler{
		resolver: resolver,
		dialer:   dialer,
		tracer:   tracer,
		metrics:  gwMetrics,
		ipFilter: ipFilter,
	}

	s := &Server{config: config}

	e := capi.SetupRouter(o, capi.WithCORS(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowHeaders: corsAllowHeaders(),
		MaxAge:       86400,
	}))
	e.Use(middleware.BodyLimit(maxRequestSize))
	e.Use(normalizeHeadersMiddleware)
	spec.RegisterHandlers(e, h)

	s.echo = e
	return s, nil
}

var headerSuffixes = []string{
	"Connection-String",
	"Array-Mode",
	"Raw-Text-Output",
	"Batch-Isolation-Level",
	"Batch-Read-Only",
	"Batch-Deferrable",
}

func corsAllowHeaders() []string {
	headers := append([]string{}, capi.DefaultAllowHeaders...)
	for _, suffix := range headerSuffixes {
		headers = append(headers, suffix, "Neon-"+suffix)
	}
	return headers
}

func normalizeHeadersMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		h := c.Request().Header
		for _, suffix := range headerSuffixes {
			if h.Get(suffix) != "" {
				continue
			}
			for key := range h {
				if strings.HasSuffix(key, "-"+suffix) {
					h.Set(suffix, h.Get(key))
					break
				}
			}
		}
		return next(c)
	}
}

func (s *Server) Run(ctx context.Context) error {
	logger := log.Ctx(ctx)

	baseListener, err := net.Listen("tcp", s.config.ListenAddress)
	if err != nil {
		return err
	}

	// Wrap with PROXY protocol support so RemoteAddr() returns the real
	// client IP when behind an AWS NLB (or any L4 proxy sending PROXY
	// protocol headers). Falls back to the actual connection address when
	// no PROXY protocol header is present.
	var listener net.Listener = &proxyproto.Listener{
		Listener: baseListener,
		ConnPolicy: func(opts proxyproto.ConnPolicyOptions) (proxyproto.Policy, error) {
			return proxyproto.USE, nil
		},
	}

	if s.config.TLSCert != nil {
		listener = tls.NewListener(listener, &tls.Config{
			Certificates: []tls.Certificate{*s.config.TLSCert},
			MinVersion:   tls.VersionTLS12,
		})
	}

	server := &http.Server{
		Handler:           s.echo,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog: o11y.StdLoggerFunc(logger, func(msg string) zerolog.Level {
			if strings.HasPrefix(msg, "http: TLS handshake error") {
				return zerolog.DebugLevel
			}
			return zerolog.WarnLevel
		}),
	}

	errChan := make(chan error, 1)
	go func() {
		logger.Info().Str("address", s.config.ListenAddress).Msg("starting HTTP/WebSocket server")
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	select {
	case <-ctx.Done():
		logger.Info().Msg("shutting down HTTP/WebSocket server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error().Err(err).Msg("HTTP server shutdown error")
			return err
		}
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}
