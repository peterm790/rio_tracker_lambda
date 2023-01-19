# Define custom function directory
ARG FUNCTION_DIR="lambda/"

FROM amazon/aws-lambda-python:3.9

# Include global arg in this stage of the build
ARG FUNCTION_DIR

#LABEL maintainer="Peter Marsh"
# Installs python, removes cache file to make things smaller
RUN yum update -y && \
    yum install -y python3 python3-dev python3-pip gcc git wget curl && \
    pip install --upgrade pip &&\
    rm -Rf /var/cache/yum &&\
    rm -rf /var/lib/apt/lists/*


#use miniconda rather than pip
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"

RUN wget \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh 

# Copies env file into the container
COPY environment.yml .

#copy shared object file into container
COPY decyb.so . 

# Installs dependencies found in the enviroment.yml file
RUN conda env create --file environment.yml --prefix /opt/conda-env

# We now replace the image’s existing Python with Python from the conda environment:
RUN mv /var/lang/bin/python3.9 /var/lang/bin/python3.9-clean && ln -sf /opt/conda-env/bin/python /var/lang/bin/python3.9

# Be sure to copy over the function itself!
# Goes last to take advantage of Docker caching.
COPY rio_tracker.py ${FUNCTION_DIR}
ENV PYTHONPATH "/var/lang/lib/python3.9/site-packages:${FUNCTION_DIR}"

# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

ENTRYPOINT [ "/var/lang/bin/python3.9", "-m", "awslambdaric" ]
# Points to the handler function of your lambda function
CMD ["rio_tracker.handler"]