# python-simples: A Python Library that Helps You Do Some Simple Works

## installation

```shell
pip install python-simples
```

## features

1. `SimpleStructure`, simple directory structure that store data and output files.
2. `SimpleCellar`, simple command line application installation that not in PATH environment.
3. `SimpleTask`, simple task that you and create it in somewhere, run it in another place.

## usage

### 1. SimpleStructure

SimpleStructure is a suggested directory structure for command line software to run.
I often have such a need, there is a directory /path/to/root,
and several subdirectories are saved in this directory, which are:

1. Data: The original data directory of the software running
2. Output: software running output directory

```python
from simples import SimpleStructure


ss = SimpleStructure('/path/to/root')
ss('stdout.txt')
ss('stderr.txt')
ss('status.txt')
ss.data_dir
ss.data('expression.txt')
ss.output_dir
ss.output('diff-exprs.txt')
```

### 2. SimpleCellar

SimpleCellar was written to solve the problem of running some command line
software that is not in the PATH environment variable. For example, bioinformatics often needs to use blast.
If your BLAST is not in the environment variable, it is very troublesome to call it.

```python
from simples import SimpleCellar


sc = SimpleCellar('/path/to/blast/install')
sc.bin_dir
sc.bin('blastn')
sc.bin('blastp')
```

### 3. SimpleTask

The original intention of SimpleTask is to solve the problem of
remote running of command line software. That is, the parameter
specification of the command line software is not in the same
place as the operation (the most typical case is the web,
for example, the user specifies the parameters in the view,
and the executed process is a Celery Task).

The solution is to:

1. Create a serializable task type
2. This type supports serialization and deserialization
3. The task object knows how to run it

So there is SimpleTask.

```python
from simples import SimpleTask, Option, Argument


# you create task in some place
st = SimpleTask('/path/to/root', 'ls')
st.add_param(Option('-l'))
st.add_param(Argument('-D', '%Y-%m-%d'))
st.add_param(Option('.))
file = st.save()

# you run in another place
new_st = SimpleTask.load_file(file)
new_st.run()
```

Files that SimpleTask generate:

1. `task.json`, task serialization json, you can deserialize task object by this file
2. `stdout.txt`, standard output message of this task
3. `stderr.txt`, standard error message of this task
4. `status.txt`, task status, -1 represents run failed, 0 represents running, 1 represent successful.
