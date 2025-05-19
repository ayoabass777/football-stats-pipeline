FROM python:3.10-slim


#Install dbt and its Postgres adapter
COPY requirements.txt /tmp/dbt-requirements.txt
RUN pip install --no-cache-dir -r /tmp/dbt-requirements.txt

# Add  dbt project code
WORKDIR /opt/dbt
COPY football_pipeline/ .

# Default entrypoint just shows dbt version
CMD ["dbt", "--version"]