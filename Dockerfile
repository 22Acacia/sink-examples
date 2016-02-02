FROM acacia

ADD sink-examples/ /app/
RUN pip install -r /app/requirements.txt

COPY ship.d /etc/ship.d
