from alpine
LABEL org.opencontainers.image.source="https://github.com/yvolchkov/pdf_collate"

RUN apk add --no-cache tini qpdf python3

COPY merge.py /opt/merge.py

ENTRYPOINT ["tini", "--"]
CMD ["/opt/merge.py"]
