#!/usr/bin/env python3

import http
import os
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request

import simulator

ADT_A01 = [
    r"MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||202401201630||ADT^A01|||2.5",
    r"PID|1||478237423||ELIZABETH HOLMES||19840203|F",
    r"NK1|1|SUNNY BALWANI|PARTNER"
]

ORU_R01 = [
    r"MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||202401201800||ORU^R01|||2.5",
    r"PID|1||478237423",
    r"OBR|1||||||202401202243",
    r"OBX|1|SN|CREATININE||103.4",
]

ADT_A03 = [
    r"MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||202401221000||ADT^A03|||2.5",
    r"PID|1||478237423",
]

ACK = [
    r"MSH|^~\&|||||20240129093837||ACK|||2.5",
    r"MSA|AA",
]

def wait_until_healthy(p, http_address):
    max_attempts = 20
    for _ in range(max_attempts):
        if p.poll() is not None:
            return False
        try:
            r = urllib.request.urlopen("http://%s/healthy" % http_address)
            if r.status == 200:
                return True
        except urllib.error.URLError:
            pass
        time.sleep(0.5)
    return False

TEST_MLLP_PORT = 18440
TEST_PAGER_PORT = 18441

def to_mllp(segments):
    m = bytes(chr(simulator.MLLP_START_OF_BLOCK), "ascii")
    m += bytes("\r".join(segments) + "\r", "ascii")
    m += bytes(chr(simulator.MLLP_END_OF_BLOCK) + chr(simulator.MLLP_CARRIAGE_RETURN), "ascii")
    return m

def from_mllp(buffer):
    return str(buffer[1:-3], "ascii").split("\r") # Strip MLLP framing and final \r

class SimulatorTest(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        messages_filename = os.path.join(self.directory, "messages.mllp")
        with open(messages_filename, "wb") as w:
            for m in (ADT_A01, ORU_R01, ADT_A03):
                w.write(to_mllp(m))
        self.simulator = subprocess.Popen([
            "./simulator.py",
            "--no_log",
            f"--mllp={TEST_MLLP_PORT}",
            f"--pager={TEST_PAGER_PORT}",
            f"--messages={messages_filename}"
        ])
        self.assertTrue(wait_until_healthy(self.simulator, f"localhost:{TEST_PAGER_PORT}"))

    def test_read_all_messages(self):
        messages = []
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("localhost", TEST_MLLP_PORT))
            while True:
                buffer = s.recv(1024)
                if len(buffer) == 0:
                    break
                messages.append(from_mllp(buffer))
                s.sendall(to_mllp(ACK))
        self.assertEqual(messages, [ADT_A01, ORU_R01, ADT_A03])

    def test_read_all_messages_with_partial_acks(self):
        messages = []
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("localhost", TEST_MLLP_PORT))
            while True:
                buffer = s.recv(1024)
                if len(buffer) == 0:
                    break
                messages.append(from_mllp(buffer))
                ack = to_mllp(ACK)
                s.sendall(ack[0:len(ack)//2])
                time.sleep(1) # Wait for TCP buffer to empty
                s.sendall(ack[len(ack)//2:])
        self.assertEqual(messages, [ADT_A01, ORU_R01, ADT_A03])

    def test_repeated_runs_return_the_same_messages(self):
        runs = []
        for _ in range(0, 3):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                messages = []
                runs.append(messages)
                s.connect(("localhost", TEST_MLLP_PORT))
                while True:
                    buffer = s.recv(1024)
                    if len(buffer) == 0:
                        break
                    messages.append(from_mllp(buffer))
                    s.sendall(to_mllp(ACK))
        for i in range(1, len(runs)):
            self.assertEqual(runs[i], runs[0])

    def test_page_with_valid_mrn(self):
        data = b"1234"
        r = urllib.request.urlopen(f"http://localhost:{TEST_PAGER_PORT}/page", data=data)
        self.assertEqual(r.status, http.HTTPStatus.OK)

    def test_page_with_bad_mrn(self):
        data = b"NHS1234"
        try:
            urllib.request.urlopen(f"http://localhost:{TEST_PAGER_PORT}/page", data=data)
        except urllib.error.HTTPError as e:
            self.assertEqual(e.status, http.HTTPStatus.BAD_REQUEST)
        else:
            self.fail("Expected /page to return an error with a bad MRN")

    def test_page_with_valid_mrn_and_valid_timestamp(self):
        data = b"1234,202401221000"
        r = urllib.request.urlopen(f"http://localhost:{TEST_PAGER_PORT}/page", data=data)
        self.assertEqual(r.status, http.HTTPStatus.OK)

    def test_page_with_valid_mrn_and_bad_timestamp(self):
        data = b"1234,2024/01/22 10:00"
        try:
            urllib.request.urlopen(f"http://localhost:{TEST_PAGER_PORT}/page", data=data)
        except urllib.error.HTTPError as e:
            self.assertEqual(e.status, http.HTTPStatus.BAD_REQUEST)
        else:
            self.fail("Expected /page to return an error with a bad timestamp")

    def test_page_with_bad_mrn_and_valid_timestamp(self):
        data = b"NHS1234,202401221000"
        try:
            urllib.request.urlopen(f"http://localhost:{TEST_PAGER_PORT}/page", data=data)
        except urllib.error.HTTPError as e:
            self.assertEqual(e.status, http.HTTPStatus.BAD_REQUEST)
        else:
            self.fail("Expected /page to return an error with a bad MRN")

    def test_page_with_too_many_values(self):
        data = b"1234,202401221000,unused"
        try:
            urllib.request.urlopen(f"http://localhost:{TEST_PAGER_PORT}/page", data=data)
        except urllib.error.HTTPError as e:
            self.assertEqual(e.status, http.HTTPStatus.BAD_REQUEST)
        else:
            self.fail("Expected /page to return an error with too many values")

    def tearDown(self):
        try:
            r = urllib.request.urlopen(f"http://localhost:{TEST_PAGER_PORT}/shutdown")
            self.assertEqual(r.status, http.HTTPStatus.OK)
            self.simulator.wait()
            self.assertEqual(self.simulator.returncode, 0)
        finally:
            if self.simulator.poll() is None:
                self.simulator.kill()
            shutil.rmtree(self.directory)

if __name__ == "__main__":
    unittest.main()
