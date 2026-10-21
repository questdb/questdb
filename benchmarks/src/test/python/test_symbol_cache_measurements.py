from contextlib import redirect_stdout
import importlib.util
import io
import json
import math
from pathlib import Path
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[2] / 'main/python'


def load(name):
    spec = importlib.util.spec_from_file_location(name, SOURCE / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


results = load('symbol_cache_results')
runner = load('symbol_cache_run')


def write_like_runs(directory, runs):
    manifest = []
    for variant, block, phases in runs:
        name = f'like-{variant}-r{block}'
        manifest.append({
            'name': name, 'stage': 'like', 'variant': variant, 'round': block,
            'command': ['java', 'org.questdb.SymbolMapCacheLikeBenchmark', '64', 'ample'],
        })
        (directory / (name + '.log')).write_text('\n'.join(
            f'SYMBOL_LIKE {phase} samples_us={json.dumps(values)} checksum={checksum}'
            for phase, values, checksum in phases
        ))
    (directory / 'manifest.json').write_text(json.dumps(manifest))


def write_query_runs(directory, runs):
    manifest = []
    for variant, block, values, checksum in runs:
        name = f'query-{variant}-r{block}'
        manifest.append({
            'name': name, 'stage': 'query', 'variant': variant, 'round': block,
            'command': [
                'java', 'org.questdb.SymbolMapCacheQueryBenchmark',
                'NOCACHE' if variant == 'nocache' else 'CACHE',
                '100', '5', '9', '1', '65536',
                '262144' if variant == 'candidate' else '268435456',
            ],
        })
        (directory / (name + '.log')).write_text('\n'.join(
            f'SYMBOL_QUERY {query} samples_us={json.dumps(values)} checksum={checksum}'
            for query in ['json', 'like', 'join', 'parallel', 'moving']
        ))
    (directory / 'manifest.json').write_text(json.dumps(manifest))
    return manifest


class SymbolCacheMeasurementsTest(unittest.TestCase):
    def test_ratio_pairs_only_matching_rounds(self):
        baseline = {0: 2, 1: 4, 2: 8, 3: 10000}
        candidate = {0: 3, 1: 6, 2: 12, 4: 10000}
        self.assertEqual('1.500 [1.500, 1.500]', results.paired_ratio(baseline, candidate))

    def test_uncertain_ratio_interval(self):
        # Logs of these ratios are -ln(2), 0, +ln(2); t(2) gives a wide interval.
        self.assertEqual('1.000 [0.179, 5.595]', results.paired_ratio(
            {0: 1, 1: 1, 2: 1}, {0: 0.5, 1: 1, 2: 2},
        ))

    def test_incomplete_comparison_has_no_interval(self):
        self.assertEqual('1.500 (n=1, no CI)', results.paired_ratio({0: 2}, {0: 3}))
        with self.assertRaisesRegex(ValueError, 'no paired JVM rounds'):
            results.paired_ratio({0: 2}, {1: 3})

    def test_six_pairs_use_five_degrees_of_freedom(self):
        baseline = dict.fromkeys(range(6), 1)
        candidate = {i: math.exp(v) for i, v in enumerate([-0.2, -0.1, 0, 0, 0.1, 0.2])}
        margin = 2.570581835636305 * math.sqrt(0.1 / 5 / 6)
        self.assertEqual(
            f'1.000 [{math.exp(-margin):.3f}, {math.exp(margin):.3f}]',
            results.paired_ratio(baseline, candidate),
        )

    def test_quartiles_interpolate_between_jvm_medians(self):
        self.assertEqual(3, results.quartile([9, 1, 5], .25))
        self.assertEqual(5, results.quartile([9, 1, 5], .5))
        self.assertEqual(7, results.quartile([9, 1, 5], .75))
        self.assertEqual(2, results.quartile([2], .25))

    def test_case_names_are_unique_and_rounds_rotate(self):
        for stage in ['reader', 'map', 'miss', 'mixed', 'fill', 'query', 'like']:
            jobs = list(runner.cases(stage, 3, [2] if stage == 'mixed' else [1], [64, 65536]))
            self.assertEqual(len(jobs), len({job[0] for job in jobs}), stage)
        variants = [job[1] for job in runner.cases('query', 3, [1], [64])]
        self.assertEqual([
            'baseline', 'candidate', 'nocache',
            'candidate', 'nocache', 'baseline',
            'nocache', 'baseline', 'candidate',
        ], variants)
        self.assertEqual([
            'baseline', 'candidate', 'candidate', 'baseline', 'baseline', 'candidate',
        ], [job[1] for job in runner.cases('like', 3, [1], [64])])

    def test_reader_matrix_keeps_requested_thread_counts(self):
        jobs = list(runner.cases('reader', 3, [1, 4, 8], [64, 65536]))
        self.assertEqual(90, len(jobs))
        self.assertEqual(90, len({job[0] for job in jobs}))
        self.assertEqual({'1', '4', '8'}, {options[options.index('-t') + 1] for _, _, _, _, options in jobs})

    def test_mixed_control_does_the_same_number_of_writes(self):
        jobs = list(runner.cases('mixed', 1, [2], [64]))
        self.assertEqual(['shared', 'separate'], [job[1] for job in jobs])
        for _, _, _, _, options in jobs:
            self.assertEqual('2', options[options.index('-t') + 1])
            self.assertEqual('1,1', options[options.index('-tg') + 1])

    def test_report_rejects_changed_query_results(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            write_query_runs(directory, [
                (variant, 0, [1, 2, 3], checksum)
                for variant, checksum in [('baseline', 10), ('candidate', 11), ('nocache', 10)]
            ])
            with self.assertRaisesRegex(ValueError, 'query result changed between runs'):
                results.main(directory)

    def test_query_report_rejects_unmatched_rounds(self):
        for candidate_rounds, nocache_rounds in [
            ([0, 1, 2], []),
            ([0, 1, 2], [0, 1]),
            ([0, 1, 3], [0, 1, 2]),
            ([0, 1, 2, 3], [0, 1, 2]),
        ]:
            with self.subTest(candidate=candidate_rounds, nocache=nocache_rounds), tempfile.TemporaryDirectory() as temp:
                directory = Path(temp)
                write_query_runs(directory, [
                    (variant, block, [1, 2, 3], 10)
                    for variant, rounds in [('baseline', [0, 1, 2]), ('candidate', candidate_rounds), ('nocache', nocache_rounds)]
                    for block in rounds
                ])
                with self.assertRaisesRegex(ValueError, 'incomplete query comparison'):
                    results.main(directory)

    def test_query_report_rejects_changed_workload_parameters(self):
        for parameter, index, value in [
            ('warmups', -6, '200'),
            ('repetitions', -5, '10'),
            ('batches', -4, '7'),
            ('workers', -3, '2'),
            ('symbols', -2, '1048576'),
        ]:
            with self.subTest(parameter=parameter), tempfile.TemporaryDirectory() as temp:
                directory = Path(temp)
                manifest = write_query_runs(directory, [
                    (variant, 0, [1, 2, 3], 10)
                    for variant in ['baseline', 'candidate', 'nocache']
                ])
                manifest[1]['command'][index] = value
                (directory / 'manifest.json').write_text(json.dumps(manifest))
                with self.assertRaisesRegex(ValueError, 'query parameters changed between runs'):
                    results.main(directory)

    def test_query_report_accepts_cache_controls_and_uses_paired_jvm_medians(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            write_query_runs(directory, [
                (variant, block, [1, median, 100], 10)
                for block in range(3)
                for variant, median in [('baseline', 2), ('candidate', 3), ('nocache', 4)]
            ])
            output = io.StringIO()
            with redirect_stdout(output):
                results.main(directory)
            self.assertIn(
                '| join | 2.000 [2.000, 2.000] (n=3) | 3.000 [3.000, 3.000] (n=3) | 4.000 [4.000, 4.000] (n=3) | 1.500 [1.500, 1.500] |',
                output.getvalue(),
            )

    def test_like_report_rejects_incomplete_results(self):
        for phases in [['first'], ['repeated'], ['first', 'first'], ['first', 'repeated']]:
            with self.subTest(phases=phases), tempfile.TemporaryDirectory() as temp:
                directory = Path(temp)
                write_like_runs(directory, [
                    ('baseline', 0, [(phase, [1, 2, 3], 10) for phase in phases]),
                ])
                with self.assertRaisesRegex(ValueError, 'incomplete LIKE'):
                    results.main(directory)

    def test_like_report_rejects_changed_results(self):
        for repeated_checksum, candidate_checksum in [(11, 10), (10, 11)]:
            with self.subTest(repeated=repeated_checksum, candidate=candidate_checksum), tempfile.TemporaryDirectory() as temp:
                directory = Path(temp)
                write_like_runs(directory, [
                    ('baseline', 0, [('first', [1, 2, 3], 10), ('repeated', [1, 2, 3], repeated_checksum)]),
                    ('candidate', 0, [('first', [1, 2, 3], candidate_checksum), ('repeated', [1, 2, 3], candidate_checksum)]),
                ])
                with self.assertRaisesRegex(ValueError, 'LIKE result changed'):
                    results.main(directory)

    def test_like_report_uses_independent_jvm_medians(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            write_like_runs(directory, [
                (variant, block, [('first', [1, median, 100], 10), ('repeated', [1, 2 * median, 200], 10)])
                for block in range(3)
                for variant, median in [('baseline', 2), ('candidate', 3)]
            ])
            output = io.StringIO()
            with redirect_stdout(output):
                results.main(directory)
            report = output.getvalue()
            self.assertIn('| first | 2.000 [2.000, 2.000] (n=3) | 3.000 [3.000, 3.000] (n=3) | 1.500 [1.500, 1.500] |', report)
            self.assertIn('| repeated | 4.000 [4.000, 4.000] (n=3) | 6.000 [6.000, 6.000] (n=3) | 1.500 [1.500, 1.500] |', report)
            self.assertNotIn('NOCACHE', report)
            self.assertNotIn('Lookup timings', report)

    def test_native_comparison_includes_development_overrides(self):
        with tempfile.TemporaryDirectory() as temp:
            cores = {variant: str(Path(temp) / variant) for variant in ['baseline', 'candidate']}
            environment = {'cores': cores}
            for core in cores.values():
                path = Path(core) / 'io/questdb/bin/darwin-aarch64/libquestdb.dylib'
                path.parent.mkdir(parents=True)
                path.write_bytes(b'production')
            runner.check_natives(environment)
            dev = Path(cores['candidate']) / 'io/questdb/bin-local/libquestdb.dylib'
            dev.parent.mkdir(parents=True)
            dev.write_bytes(b'development')
            with self.assertRaisesRegex(ValueError, 'native libraries differ'):
                runner.check_natives(environment)


if __name__ == '__main__':
    unittest.main()
