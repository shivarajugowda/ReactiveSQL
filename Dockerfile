# Docker Multi-Stage build to reduce image size.
# Build compile Image to setup venv with required python packages.
FROM python:3.7.6-alpine AS compile-image
RUN apk add --no-cache --virtual .build-deps gcc musl-dev make

# Use Viertual env to mimize size.
ENV PATH="/usr/src/venv/bin:$PATH"
RUN python -m venv /usr/src/venv

# install requirements
WORKDIR /usr/src
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

###########################################################
# Build  Target Image.
FROM python:3.7.6-alpine AS build-image

# Copy virtualenv and set it up:
COPY --from=compile-image /usr/src/venv /usr/src/venv
ENV PATH="/usr/src/venv/bin:$PATH"

# add app
WORKDIR /usr/src
COPY app /usr/src/app
