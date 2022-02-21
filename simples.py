# -*- coding: utf-8 -*-
import json
import traceback
from pathlib import Path

from sh import Command


class SimpleError(Exception):
    """Raised when python-simples errors occurred."""


def mkdir(dir):
    """make directory and return it."""
    dir.mkdir(parents=True, exist_ok=True)
    return dir


def is_str(value):
    if not isinstance(value, str):
        raise SimpleError(f'{value}不是字符型。')
    return value


class RootMixin:
    def __init__(self, root):
        if isinstance(root, str):
            root = Path(root)
        self.root = mkdir(root)


class SimpleStructure(RootMixin):
    """
    simple directory structure, sub-directories:
    1. Data: store raw data
    2. Output: store data outputs

    eg:
    ss = SimpleStructure('/path/to/root')
    ss('stdout.txt')  # /path/to/root/stdout.txt
    ss('stderr.txt')  # /path/to/root/stderr.txt
    ss('status.txt')  # /path/to/root/status.txt
    ss.data_dir  # /path/to/root/Data
    ss.data('expression.txt')  # /path/to/root/Data/expression.txt
    ss.output_dir  # /path/to/root/expression.txt
    ss.output('diff-genes.txt')  # /path/to/root/Output/diff-genes.txt
    """
    def __call__(self, filename):
        """file that under the root"""
        return self.root / filename

    @property
    def data_dir(self):
        """data directory"""
        return mkdir(self.root / 'Data')

    def data(self, filename):
        """file that under data directory"""
        return self.data_dir / filename

    @property
    def output_dir(self):
        """output directory"""
        return mkdir(self.root / 'Output')

    def output(self, filename):
        """file that under output directory"""
        return self.output_dir / filename


class SimpleCellar(RootMixin):
    """Simple command-line application installation"""
    @property
    def bin_dir(self):
        """bin directory"""
        return self.root / 'bin'

    def bin(self, filename):
        """file that under bin directory"""
        return self.bin_dir / filename


class Parameter:
    def __iter__(self):
        raise NotImplementedError

    def dict(self):
        raise NotImplementedError

    @classmethod
    def check(cls, data):
        raise NotImplementedError

    @classmethod
    def load(cls, data):
        return cls(**data)


class Argument(Parameter):
    """Command Argument, eg, <-i some-file.txt>"""
    def __init__(self, key, value):
        """
        Args:
            key: argument key, usually starts with a "-", eg, -i
            value: argument value, eg, some-file.txt
        """
        self.key = is_str(key)
        self.value = is_str(value)

    def __iter__(self):
        return iter([self.key, self.value])

    def dict(self):
        return {
            'key': self.key,
            'value': self.value
        }

    @classmethod
    def check(cls, data):
        if len(data) != 2:
            return False
        return ('key' in data) and ('value' in data)


class MultiArguments(Parameter):
    """Multiple command arguments, eg, <-i foo.txt -i bar.txt>"""
    def __init__(self, key, values):
        """
        Args:
            key: argument key, eg, -i
            values: argument values, eg: ["foo.txt", "bar.txt"]
        """
        self.key = is_str(key)
        self.values = [is_str(value) for value in values]

    def __iter__(self):
        data = []
        for value in self.values:
            data.append(self.key)
            data.append(value)
        return iter(data)

    @classmethod
    def check(cls, data):
        if len(data) != 2:
            return False
        return ('key' in data) and ('values' in data) and isinstance(data['values'], list)


class Option(Parameter):
    """command option, eg, <foo.txt>"""
    def __init__(self, value):
        """
        Args:
            value: options value, eg, foo.txt
        """
        self.value = is_str(value)

    def __iter__(self):
        return iter([self.value])

    def dict(self):
        return {
            'value': self.value
        }

    @classmethod
    def check(cls, data):
        if len(data) != 1:
            return False
        return 'value' in data


class MultiOptions(Parameter):
    """multiple command options, eg, <foo.txt bar.txt>"""
    def __init__(self, values):
        """
        Args:
            values: option values, eg, ["foo.txt", "bar.txt"]
        """
        self.values = [is_str(value) for value in values]

    def __iter__(self):
        return iter(self.values)

    def dict(self):
        return {
            'values': self.values
        }

    @classmethod
    def check(cls, data):
        if len(data) != 1:
            return False
        return ('values' in data) and isinstance(data['values'], list)


class ParameterCreator:
    """create parameter object according data itself."""
    param_classes = [Argument, MultiArguments, Option, MultiOptions]

    @classmethod
    def create(cls, data):
        """
        ParameterCreator will auto detect the right parameter type,
        eg, {"key": "-i", "value": "foo.txt"} will be detected as Argument.
        {"key": "-i", "values": ["foo.txt", "bar.txt"]} will be detected as MultiArguments.
        {"value": "foo.txt"} will be detected as Option.
        {"values": ["foo.txt", "bar.txt"} will be detected as MultiOptions.

        Args:
            data: parameter data
        """
        for param_class in cls.param_classes:
            if param_class.check(data):
                return param_class.load(data)
        raise TypeError(f"can't detect parameter type.")


class SimpleTask:
    """
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
    """
    def __init__(self, root, command, params=None):
        """
        Args:
            root: task root, everything about a task is saved into this directory.
            command: command this task will run
            params: command parameters, can be None, specify it using add_param then.
        """
        self.ss = SimpleStructure(root)

        if isinstance(command, str):
            command = Command(command)
        self.command = command

        if params is None:
            params = []
        self.params = params

    def __iter__(self):
        data = []
        for param in self.params:
            for item in param:
                data.append(item)
        return iter(data)

    def __repr__(self):
        return ' '.join([str(self.command), *list(self)])

    @property
    def root(self):
        return self.ss.root

    def __call__(self, filename):
        return self.ss(filename)

    def data_dir(self):
        return self.ss.data_dir

    def data(self, filename):
        return self.ss.data(filename)

    @property
    def output_dir(self):
        return self.ss.output_dir

    def output(self, filename):
        return self.ss.output(filename)

    @property
    def stdout_file(self):
        return self('stdout.txt')

    @property
    def stdout(self):
        if self.stdout_file.exists():
            with self.stdout_file.open() as fp:
                return fp.read()
        return None

    def save_stdout(self, *stdouts):
        with self.stdout_file.open('w') as fp:
            for stdout in stdouts:
                if isinstance(stdout, bytes):
                    stdout = stdout.decode('utf-8')
                fp.write(stdout)
                fp.write('\n')

    @property
    def stderr_file(self):
        return self('stderr.txt')

    @property
    def stderr(self):
        if self.stderr_file.exists():
            with self.stderr_file.open() as fp:
                return fp.read()
        return None

    def save_stderr(self, *stderrs):
        with self.stderr_file.open('w') as fp:
            for stderr in stderrs:
                if isinstance(stderr, bytes):
                    stderr = stderr.decode('utf-8')
                fp.write(stderr)
                fp.write('\n')

    @property
    def status_file(self):
        return self('status.txt')

    @property
    def status(self):
        if self.status_file.exists():
            with self.status_file.open() as fp:
                return int(fp.read().rstrip())
        return None

    def save_status(self, status):
        with self.status_file.open('w') as fp:
            fp.write(str(status))

    def set_running(self):
        self.save_status(0)

    def set_successful(self):
        self.save_status(1)

    def set_failed(self):
        self.save_status(-1)

    def is_running(self):
        return self.status == 0

    def is_failed(self):
        return self.status == -1

    def is_successful(self):
        return self.status == 1

    def add_param(self, param):
        self.params.append(param)

    def run(self):
        try:
            self.set_running()
            proc = self.command(*list(self))
            self.save_stdout(proc.stdout)
            self.save_stderr(proc.stderr)
            self.set_successful()
            return True
        except Exception:
            self.save_stderr(traceback.format_exc())
            self.set_failed()
            return False

    def dict(self):
        return {
            'root': str(self.root),
            'command': str(self.command),
            'params': [param.dict() for param in self.params]
        }

    def save(self):
        file = self.root / 'task.json'
        with file.open('w') as fp:
            json.dump(self.dict(), fp, indent=4)
        return file

    @classmethod
    def load(cls, data):
        task = cls(root=data['root'], command=data['command'])
        for param_data in data['params']:
            param = ParameterCreator.create(param_data)
            task.add_param(param)
        return task

    @classmethod
    def load_file(cls, file):
        if isinstance(file, str):
            file = Path(file)
        with file.open() as fp:
            data = json.load(fp)
        return cls.load(data)