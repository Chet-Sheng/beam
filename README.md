# Setup Apache Beam Environment with Docker
Build Image
```bash
docker build -t $(basename $("pwd")) .
```

Start Container
```bash
docker run -it -u $(id -u):$(id -g) \
               --rm \
               --network="host" \
               -v /$(pwd):/repo \
               -w /repo \
               --name $(basename $("pwd"))\
               $(basename $("pwd")):latest
```
Access Running Container
```bash
docker exec -it $(basename $("pwd")) bash
```

# Examples
Directory Structure
```bash
beam
└── examples
    ├── __init__.py
    ├── __pycache__
    │   ├── __init__.cpython-37.pyc
    │   └── wordcount_minimal.cpython-37.pyc
    ├── data
    │   └── wordcount_minimal-00000-of-00001
    └── wordcount_minimal.py
```
### wordcount_minimal.py
- Applying ParDo with an explicit DoFn
- Creating Composite Transforms
- Using Parameterizable PipelineOptions
```bash
# in directory beam, run following command:
python -m examples.wordcount_minimal --output ./examples/data/wordcount_minimal
```
### wordcount.py
```bash
python -m examples.wordcount --output ./examples/data/wordcount
```
### wordcount_debugging.py
```bash
python -m apache_beam.examples.wordcount_debugging --output ./examples/data/wordcount_debugging
```


