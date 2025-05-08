ARG BUILD_FROM
FROM $BUILD_FROM

COPY requirements.txt /
RUN pip install -r requirements.txt

COPY docker_entrypoint.sh /
RUN chmod a+x /docker_entrypoint.sh

# Copy data for add-on
COPY ha-wb-discovery.py /
COPY ha_wb_discovery/ /ha_wb_discovery

CMD [ "/docker_entrypoint.sh" ]