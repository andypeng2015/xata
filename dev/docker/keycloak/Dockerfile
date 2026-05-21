# Stage 1: Build keycloak-turnstile plugin from local source
FROM maven:3.9-eclipse-temurin-17 AS turnstile-builder
WORKDIR /build
COPY keycloak-turnstile/ .
RUN mvn clean package -DskipTests

# Stage 2: Download theme
FROM registry.access.redhat.com/ubi9 AS theme-download
# TODO GIT_TOKEN should use build secrets
ARG GIT_TOKEN
ARG NO_THEME

# Get theme file
ADD RELEASE ./RELEASE
COPY . /build-context/
RUN if [ ! -z "$NO_THEME" ]; then \
        echo "Skipping theme download"; \
    elif [ -f "/build-context/xata-theme.jar" ]; then \
        echo "Using local xata-theme.jar file"; \
        cp /build-context/xata-theme.jar /xata-theme.jar; \
    elif [ ! -z "$GIT_TOKEN" ]; then \
        echo "Local xata-theme.jar not found, downloading from GitHub"; \
        source ./RELEASE && \
            curl -f -L \
            -H "Authorization: Bearer $GIT_TOKEN" \
            -H "Accept: application/octet-stream" \
            -o /xata-theme.jar \
            $AUTH_THEME_ASSET_URL; \
    else \
        echo "Error: Custom theme could not be downloaded from xataio/frontend releases. set GIT_TOKEN to a valid github token"; \
        exit 1; \
    fi

# Stage 3: Build Keycloak with plugins
FROM quay.io/keycloak/keycloak:26.6.2 AS builder

# Enable health and metrics support
ENV KC_HEALTH_ENABLED=true
ENV KC_METRICS_ENABLED=true

COPY --from=theme-download --chown=keycloak:keycloak --chmod=644 /xata-theme.jar* /opt/keycloak/providers/xata-theme.jar
COPY --from=turnstile-builder --chown=keycloak:keycloak --chmod=644 /build/target/keycloak-turnstile-jar-with-dependencies.jar /opt/keycloak/providers/keycloak-turnstile.jar

# Configure a database vendor
ENV KC_DB=postgres
ARG JAVA_TOOL_OPTIONS

WORKDIR /opt/keycloak

RUN /opt/keycloak/bin/kc.sh build --features=docker,authorization --verbose

# Stage 4: Runtime image
FROM quay.io/keycloak/keycloak:26.6.2
COPY --from=builder /opt/keycloak/ /opt/keycloak/

# change these values to point to a running postgres instance
ENV KC_DB=postgres

ENTRYPOINT ["/opt/keycloak/bin/kc.sh"]
