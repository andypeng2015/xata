package api

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/ziflex/lecho/v3"

	"xata/internal/o11y"
	"xata/openapi"
)

type Option interface {
	apply(*options)
}

type options struct {
	withRecovery bool
	corsConfig   *middleware.CORSConfig
}

type optionFunc func(*options)

func (fn optionFunc) apply(opts *options) { fn(opts) }

func WithRecovery(b bool) Option {
	return optionFunc(func(opts *options) {
		opts.withRecovery = b
	})
}

func WithCORS(cfg middleware.CORSConfig) Option {
	return optionFunc(func(opts *options) {
		opts.corsConfig = &cfg
	})
}

var DefaultAllowHeaders = []string{
	"Origin",
	"Content-Length",
	"Content-Type",
	"Authorization",
	"User-Agent",
	"X-Xata-Client-ID",
	"X-Xata-Session-ID",
	"X-Xata-Agent",
	"X-Features",
	"Timing-Allow-Origin",
}

func SetupRouter(o *o11y.O, with ...Option) *echo.Echo {
	const maxRequestIDSize = 100

	opts := options{
		withRecovery: true,
	}
	for _, opt := range with {
		opt.apply(&opts)
	}

	e := echo.New()
	e.Pre(middleware.RemoveTrailingSlash())

	logger := o.Logger()

	e.HideBanner = true
	e.Logger = lecho.From(logger)
	e.Validator = newEchoValidator()
	e.HTTPErrorHandler = makeHTTPErrorHandler()
	e.Use(requestIDMiddleware(echo.HeaderXRequestID, maxRequestIDSize))
	e.Use(o11y.LoggerMiddleware(&logger))
	if opts.withRecovery {
		e.Use(o11y.RecoverMiddleware(0))
	}
	e.Use(o11y.MetricsMiddleware(o))
	e.Use(o11y.TracingMiddleware(o))
	e.Use(xataEchoMiddleware(o))

	if opts.corsConfig != nil {
		e.Use(middleware.CORSWithConfig(*opts.corsConfig))
	} else {
		corsConfig := middleware.DefaultCORSConfig
		corsConfig.AllowHeaders = DefaultAllowHeaders
		corsConfig.MaxAge = 7200 // 2 hours
		e.Use(middleware.CORSWithConfig(corsConfig))
	}

	grp := e.Group("")
	grp.Use(o11y.MetricsMiddleware(o))
	grp.Use(o11y.TracingMiddleware(o))
	grp.Use(o11y.RecoverMiddleware(0))
	grp.GET("/_hello", hello)
	grp.GET("/openapi.yaml", serveOpenAPIYAML)
	grp.GET("/openapi.json", serveOpenAPIJSON)

	return e
}

// serveOpenAPIYAML serves the public OpenAPI spec as YAML
func serveOpenAPIYAML(c echo.Context) error {
	c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextPlain)
	return c.String(http.StatusOK, openapi.OpenAPIYAML)
}

// serveOpenAPIJSON serves the public OpenAPI spec as JSON
func serveOpenAPIJSON(c echo.Context) error {
	c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	return c.JSON(http.StatusOK, json.RawMessage(openapi.OpenAPIJSON))
}

func hello(c echo.Context) error {
	return c.JSON(http.StatusOK, struct {
		Server  string
		Version string
	}{"Xata", "0.0.1"})
}
