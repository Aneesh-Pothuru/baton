FROM python:3.12-slim AS build

WORKDIR /build
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN python -m pip wheel --no-deps --wheel-dir /wheels .

FROM python:3.12-slim

RUN useradd --create-home --uid 10001 baton
WORKDIR /app
COPY --from=build /wheels /wheels
RUN python -m pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels
COPY --chown=baton:baton docs ./docs
RUN mkdir -p /app/state && chown baton:baton /app/state

USER baton
ENV PYTHONUNBUFFERED=1 \
    BATON_HOST=0.0.0.0 \
    BATON_PORT=8020 \
    BATON_DATABASE=/app/state/baton.sqlite \
    BATON_STATIC_DIR=/app/docs
EXPOSE 8020
VOLUME ["/app/state"]
HEALTHCHECK --interval=20s --timeout=3s --start-period=5s --retries=3 \
  CMD ["python", "-c", "from urllib.request import urlopen; urlopen('http://127.0.0.1:8020/readyz', timeout=2).read()"]
CMD ["baton", "serve"]
