"""Execute the deployed shell with an isolated fake Docker command boundary."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = Path(os.environ.get('COLOMBO_DEPLOY_WORKFLOW', ROOT / '.github/workflows/backend-deploy.yml'))
DIGEST = 'sha256:' + 'a' * 64
IMAGE_ID = 'sha256:' + 'b' * 64


def deploy_script():
    workflow = WORKFLOW.read_text()
    script = workflow.split('          script: |\n', 1)[1].split('\n  resolve-clouseau:', 1)[0]
    script = textwrap.dedent(script).strip()
    prefix = "/bin/bash -lc '"
    if not script.startswith(prefix) or not script.endswith("'"):
        raise AssertionError('Expected the actual SSH bash deployment body')
    return script[len(prefix):-1].replace('${{ github.repository }}', 'fixture/colombo')


class DeploymentDigestContract(unittest.TestCase):
    def deploy(self, digest=DIGEST, mismatch=False, missing_image=False):
        with tempfile.TemporaryDirectory(prefix='colombo-digest-test-') as tmp:
            root = Path(tmp)
            (root / 'certificate').touch()
            (root / 'private-key').touch()
            docker = root / 'docker'
            docker.write_text('''#!/usr/bin/env python3
import json,os,sys
from pathlib import Path
root=Path(os.environ['DOCKER_FIXTURE'])
a=sys.argv[1:]
with (root/'calls').open('a') as out: out.write(json.dumps(a)+'\\n')
if a[:2]==['image','inspect']:
 print('' if os.environ['MISSING_IMAGE']=='1' else os.environ['EXPECTED_IMAGE'])
elif a and a[0]=='inspect':
 print(('sha256:'+'c'*64 if os.environ['MISMATCH']=='1' else os.environ['EXPECTED_IMAGE']) if (root/'started').exists() else 'sha256:'+'d'*64)
elif a and a[0]=='run':
 (root/'started').touch()
 print('fixture-container')
elif a and a[0]=='login':
 sys.stdin.read()
''')
            docker.chmod(0o755)
            env = dict(PATH=f'{root}:/usr/bin:/bin', DOCKER_FIXTURE=str(root), EXPECTED_IMAGE=IMAGE_ID,
                       MISMATCH=str(int(mismatch)), MISSING_IMAGE=str(int(missing_image)),
                       FTP_PASSIVE_EXTERNAL_ADDRESS='127.0.0.1', DEPLOY_HOST_ADDRESS='127.0.0.1',
                       FTPS_CERT=str(root / 'certificate'), FTPS_KEY=str(root / 'private-key'), DEPLOYMENT_TOKEN='fixture-only', REGISTRY_USER='fixture',
                       IMAGE_DIGEST=digest, HTTP_PORT='8080', FTP_PORT='2121', FTP_PASSIVE_PORTS='60000-60100',
                       DATABASE_URL='fixture-database', DATABASE_USER='fixture', DATABASE_PASSWORD='fixture-only',
                       FLYWAY_ENABLED='true', COLOMBO_METRICS_TOKEN='fixture-only', BUILD_VERSION='fixture-source-sha')
            result = subprocess.run(['/bin/bash', '-c', deploy_script()], env=env, capture_output=True, text=True, timeout=10)
            calls = [json.loads(line) for line in (root/'calls').read_text().splitlines()] if (root/'calls').exists() else []
            return result, calls

    def test_missing_digest_refuses_before_pull_or_container_mutation(self):
        result, calls = self.deploy(digest='')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(call[0] in ('pull', 'stop', 'rm', 'run') for call in calls))

    def test_malformed_digest_refuses_before_pull_or_container_mutation(self):
        for digest in ('latest', 'sha256:short', 'sha256:' + 'G' * 64):
            with self.subTest(digest=digest):
                result, calls = self.deploy(digest=digest)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(any(call[0] in ('pull', 'stop', 'rm', 'run') for call in calls))

    def test_exact_digest_is_pulled_run_and_verified_with_source_receipt(self):
        result, calls = self.deploy()
        self.assertEqual(result.returncode, 0, result.stderr)
        target = 'ghcr.io/fixture/colombo@' + DIGEST
        self.assertIn(['pull', target], calls)
        self.assertTrue(any(call[0] == 'run' and call[-1] == target for call in calls))
        self.assertFalse(any(':latest' in argument for call in calls for argument in call))
        self.assertIn('rollback_image_id=sha256:' + 'd' * 64, result.stdout)
        self.assertIn(f'revision=fixture-source-sha digest={DIGEST} running_image_id={IMAGE_ID}', result.stdout)

    def test_container_image_mismatch_fails_and_stops_the_unverified_container(self):
        result, calls = self.deploy(mismatch=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(calls[-1], ['stop', 'colombo-backend'])
        self.assertIn('does not match the verified digest', result.stdout)

    def test_missing_pulled_identity_refuses_before_stopping_existing_container(self):
        result, calls = self.deploy(missing_image=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(call[0] in ('stop', 'rm', 'run') for call in calls))


if __name__ == '__main__':
    unittest.main()
