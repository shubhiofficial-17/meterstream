FROM python:3.12-slim AS build
RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"
WORKDIR /src
COPY pyproject.toml README.md ./
COPY meterstream ./meterstream
RUN pip install --no-cache-dir .

FROM python:3.12-slim
RUN useradd --create-home --uid 10001 app
COPY --from=build /venv /venv
ENV PATH="/venv/bin:$PATH" PYTHONUNBUFFERED=1
USER app
WORKDIR /home/app
EXPOSE 8000 9100
# One image, three roles: `meterstream api` (default), `meterstream worker`, `meterstream simulate`
ENTRYPOINT ["meterstream"]
CMD ["api", "--port", "8000"]
