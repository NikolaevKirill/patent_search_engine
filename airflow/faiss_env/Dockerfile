FROM continuumio/anaconda3:2019.10

RUN wget --no-check-certificate -O /tmp/mklml.tgz https://github.com/intel/mkl-dnn/releases/download/v0.12/mklml_lnx_2018.0.1.20171227.tgz && \
    tar -zxvf /tmp/mklml.tgz && \
    cp -rf mklml_*/* /usr/local/ && \
    rm -rf mklml_*

RUN conda install faiss-cpu -c pytorch

WORKDIR /app
RUN mkdir -p /app/saved/

COPY requirements.txt .
RUN conda install -c conda-forge --file requirements.txt

COPY . .
COPY faiss_data/ /app/saved

CMD ["python", "main.py"]