package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"

	"xata/internal/o11y"
)

func TestRouterTrailingSlash(t *testing.T) {
	o := o11y.NewTestService(t)
	e := SetupRouter(&o)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/_hello/", nil)
	e.ServeHTTP(rec, req)

	var resp struct {
		Server string
	}
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	if err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	assert.Equal(t, "Xata", resp.Server)
}

func TestRouterCORS(t *testing.T) {
	o := o11y.NewTestService(t)
	e := SetupRouter(&o)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/anything", nil)
	req.Header.Set("Origin", "https://foo.example")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "Content-Type")
	e.ServeHTTP(rec, req)

	assert.Equal(t, rec.Result().StatusCode, http.StatusNoContent)
	assert.Equal(t, []string{"*"}, rec.Header()["Access-Control-Allow-Origin"])
	assert.Equal(t, []string{"Origin,Content-Length,Content-Type,Authorization,User-Agent,X-Xata-Client-ID,X-Xata-Session-ID,X-Xata-Agent,X-Features,Timing-Allow-Origin"}, rec.Header()["Access-Control-Allow-Headers"])
	assert.Equal(t, []string{"GET,HEAD,PUT,PATCH,POST,DELETE"}, rec.Header()["Access-Control-Allow-Methods"])
	assert.Equal(t, []string{"7200"}, rec.Header()["Access-Control-Max-Age"])
}

func TestOpenAPISpecHandlers(t *testing.T) {
	o := o11y.NewTestService(t)
	e := SetupRouter(&o)

	tests := []struct {
		name        string
		path        string
		contentType string
	}{
		{"yaml", "/openapi.yaml", echo.MIMETextPlain},
		{"json", "/openapi.json", echo.MIMEApplicationJSON},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			e.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
			assert.Equal(t, tt.contentType, rec.Header().Get(echo.HeaderContentType))
			assert.NotEmpty(t, rec.Body.String())
		})
	}
}
