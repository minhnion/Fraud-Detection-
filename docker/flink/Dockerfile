FROM apache/flink:1.18.0-scala_2.12-java11

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

RUN pip install apache-flink==1.18.0 scikit-learn==1.2.2 pandas==1.5.3 joblib==1.2.0

RUN ln -s /usr/bin/python3 /usr/bin/python

USER flink