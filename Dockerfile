FROM maven:3.9.11-eclipse-temurin-21 AS builder
WORKDIR /app

COPY . .
RUN chmod +x mvnw && ./mvnw -DskipTests package

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN apk add --no-cache bash postgresql-client

COPY --from=builder /app/target/*.jar app.jar
COPY --from=builder /app/scripts/tenants-cli.sh /usr/local/bin/tenants-cli
RUN chmod +x /usr/local/bin/tenants-cli

ARG PORT=8080
ARG COLOMBO_FTP_PORT=21
ARG COLOMBO_FTP_PASSIVE_PORTS=60000-60100

# HTTP / actuator (override at build time with --build-arg PORT=...)
EXPOSE ${PORT}

# FTP control channel (override at build time with --build-arg COLOMBO_FTP_PORT=...)
EXPOSE ${COLOMBO_FTP_PORT}

# FTP passive data ports (override at build time with --build-arg COLOMBO_FTP_PASSIVE_PORTS=...)
EXPOSE ${COLOMBO_FTP_PASSIVE_PORTS}

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
  CMD wget -qO- "http://127.0.0.1:${PORT:-8080}/actuator/health" || exit 1

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
