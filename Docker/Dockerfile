FROM python:3-slim

WORKDIR ../pydxanalyze

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir dxanalyze && mkdir delphixpy && mkdir /process && mkdir /output

COPY pydxanalyze.py .
COPY dxanalyze/ dxanalyze/
COPY delphixpy/ delphixpy/
COPY pydxanalyze-template.pptx .
COPY pydxfarmanalyze-template.pptx .

RUN chmod +x ./pydxanalyze.py
RUN ln -s ./pydxanalyze.py pydxfarmanalyze
RUN ln -s ./pydxanalyze.py pydxhealthcheck

ENV PATH $PATH:/pydxanalyze:/pydxfarmanalyze
