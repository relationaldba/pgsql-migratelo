FROM postgres:alpine

# Set working directory
RUN mkdir /opt/pgsql-migratelo
WORKDIR /opt/pgsql-migratelo

# Copy the scripts, config
COPY src/pgsql-migratelo.sh .
RUN chmod +x pgsql-migratelo.sh

# Run the script when the container starts
ENTRYPOINT ["/bin/bash","pgsql-maintenance.sh"]
# CMD ["migrate_lo"]

