# -*- coding: utf-8 -*-
from pathlib import Path
from tempfile import TemporaryDirectory
from grpc import insecure_channel

import pytest

import simples


def test_simple_structure():
    with TemporaryDirectory() as tempdir:
        tempdir = Path(tempdir)

        ss = simples.SimpleStructure(tempdir)
        assert ss('stdout.txt') == (tempdir / 'stdout.txt')
        assert ss('stderr.txt') == (tempdir / 'stderr.txt')
        assert ss('status.txt') == (tempdir / 'status.txt')
        assert ss.data_dir == (tempdir / 'Data')
        assert ss.data('expression.txt') == (tempdir / 'Data' / 'expression.txt')
        assert ss.output_dir == (tempdir / 'Output')
        assert ss.output('diff-genes.txt') == (tempdir / 'Output' / 'diff-genes.txt')


def test_simple_cellar():
    with TemporaryDirectory() as tempdir:
        tempdir = Path(tempdir)

        sc = simples.SimpleCellar(tempdir)
        assert sc.bin_dir == (tempdir / 'bin')
        assert sc.bin('blastn') == (tempdir / 'bin' / 'blastn')


def test_argument():
    with pytest.raises(simples.SimpleError):
        simples.Argument('-m', 90)

    a = simples.Argument('-m', '90')
    assert list(a) == ['-m', '90']

    assert simples.Argument.check({'key': '-m', 'value': '90'})
    assert not simples.Argument.check({'key': '-m', 'values': ['90', '80']})

    a = simples.Argument.load({'key': '-m', 'value': '90'})
    assert a.key == '-m'
    assert a.value == '90'


def test_multi_arguments():
    a = simples.MultiArguments('-i', ['foo.txt', 'bar.txt'])
    assert list(a) == ['-i', 'foo.txt', '-i', 'bar.txt']

    assert simples.MultiArguments.check({'key': '-i', 'values': ['foo.txt', 'bar.txt']})
    assert not simples.MultiArguments.check({'key': '-i', 'value': 'foo.txt'})

    a = simples.MultiArguments.load({
        'key': '-i',
        'values': ['foo.txt', 'bar.txt']
    })
    assert a.key == '-i'
    assert a.values == ['foo.txt', 'bar.txt']


def test_option():
    a = simples.Option('foo.txt')
    assert list(a) == ['foo.txt']

    assert simples.Option.check({'value': 'foo.txt'})
    assert not simples.Option.check({'values': ['foo.txt', 'bar.txt']})

    a = simples.Option.load({'value': 'foo.txt'})
    assert a.value == 'foo.txt'


def test_multi_options():
    a = simples.MultiOptions(['foo.txt', 'bar.txt'])
    assert list(a) == ['foo.txt', 'bar.txt']

    assert simples.MultiOptions.check({'values': ['foo.txt', 'bar.txt']})
    assert not simples.MultiOptions.check({'values': 'foo.txt'})

    a = simples.MultiOptions.load({'values': ['foo.txt', 'bar.txt']})
    assert a.values == ['foo.txt', 'bar.txt']


def test_parameter_creator():
    data1 = {'key': '-i', 'value': 'foo.txt'}
    data2 = {'key': '-i', 'values': ['foo.txt', 'bar.txt']}
    data3 = {'value': 'foo.txt'}
    data4 = {'values': ['foo.txt', 'bar.txt']}

    p1 = simples.ParameterCreator.create(data1)
    p2 = simples.ParameterCreator.create(data2)
    p3 = simples.ParameterCreator.create(data3)
    p4 = simples.ParameterCreator.create(data4)

    assert isinstance(p1, simples.Argument)
    assert p1.key == '-i'
    assert p1.value == 'foo.txt'

    assert isinstance(p2, simples.MultiArguments)
    assert p2.key == '-i'
    assert p2.values == ['foo.txt', 'bar.txt']

    assert isinstance(p3, simples.Option)
    assert p3.value == 'foo.txt'

    assert isinstance(p4, simples.MultiOptions)
    assert p4.values == ['foo.txt', 'bar.txt']



def test_create_simple_task():
    with TemporaryDirectory() as tempdir:
        tempdir = Path(tempdir)
        task = simples.SimpleTask(tempdir, 'ls')
        task.add_param(simples.Option('-l'))

        assert not (tempdir / 'task.json').exists()
        file = task.save()
        assert (tempdir / 'task.json').exists()

        new_task = simples.SimpleTask.load_file(file)
        assert task.root == new_task.root
        assert task.command == new_task.command

        assert not (tempdir / 'status.txt').exists()
        assert task.run()
        assert (tempdir / 'status.txt').exists()