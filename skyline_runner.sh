#!/bin/bash

USER=${PRINCIPAL}@${DOMAIN}
KEYTAB=/etc/conf/${PRINCIPAL}.keytab
export SKYLINE_INPUT_TOPIC=${SKYLINE_INPUT_TOPIC}
export SKYLINE_ERROR_TOPIC=${SKYLINE_ERROR_TOPIC}
export HOSTNAME=${HOSTNAME}

echo "INPUT_TOPIC as set in shell ${SKYLINE_INPUT_TOPIC}"
echo "ERROR_TOPIC as set in shell ${SKYLINE_ERROR_TOPIC}"

java -cp /opt/skyline/JoinAction-4.1.1-standalone.jar:/opt/skyline/target/classes acme.JoinAction ${SKYLINE_INPUT_TOPIC} $SKYLINE_OUTPUT_TOPIC} ${SKYLINE_ERROR_TOPIC}
