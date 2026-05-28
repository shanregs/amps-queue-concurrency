# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /workspace

COPY mvnw mvnw.cmd ./
COPY .mvn .mvn
COPY pom.xml .

# Download dependencies first (layer-cached)
RUN ./mvnw dependency:go-offline -q

COPY src src
RUN ./mvnw clean package -DskipTests -q

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Non-root user
RUN addgroup -S amps && adduser -S amps -G amps
USER amps

COPY --from=build /workspace/target/amps-queue-concurrency-*.jar app.jar

# Bookmark store directory (mounted as volume for persistence)
RUN mkdir -p /home/amps/.amps/bookmarks

EXPOSE 8080

# SPRING_PROFILES_ACTIVE is set per-service in docker-compose.yml
ENTRYPOINT ["java", \
  "-XX:+UseZGC", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Dfile.encoding=UTF-8", \
  "-jar", "app.jar"]
