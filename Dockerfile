FROM python:3.7 as pre

WORKDIR /repo

FROM pre AS base
COPY ./requirement.txt /repo
RUN pip install --upgrade pip setuptools && pip install -r requirement.txt
CMD ["/bin/bash"]
