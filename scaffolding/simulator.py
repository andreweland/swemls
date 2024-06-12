#!/usr/bin/env python3

import argparse
import datetime
import http.server
import logging
import signal
import socket
import sys
import threading
import time

logger = logging.getLogger(__name__)

VERSION = "0.0.0"
MLLP_BUFFER_SIZE = 1024
MLLP_TIMEOUT_SECONDS = 10
SHUTDOWN_POLL_INTERVAL_SECONDS = 2

def serve_mllp_client(client, source, messages, shutdown_mllp, short_messages):
    i = 0
    buffer = b""
    while i < len(messages) and not shutdown_mllp.is_set():
        try:
            mllp = bytes(chr(MLLP_START_OF_BLOCK), "ascii")
            mllp += messages[i]
            mllp += bytes(chr(MLLP_END_OF_BLOCK) + chr(MLLP_CARRIAGE_RETURN), "ascii")
            if not short_messages:
                client.sendall(mllp)
            else:
                client.sendall(mllp[:len(mllp)//2])
                time.sleep(1)
                client.sendall(mllp[len(mllp)//2:])
            received = []
            while len(received) < 1:
                r = client.recv(MLLP_BUFFER_SIZE)
                if len(r) == 0:
                    raise Exception("client closed connection")
                buffer += r
                received, buffer = parse_mllp_messages(buffer, source)
            acked, error = verify_ack(received)
            if error:
                raise Exception(error)
            elif acked:
                i += 1
            else:
                logger.info(f"mllp: {source}: message not acknowledged")
        except Exception as e:
            logger.error(f"mllp: {source}: {e}")
            logger.info(f"mllp: {source}: closing connection: error")
            break
    else:
        if i == len(messages):
            logger.info(f"mllp: {source}: closing connection: end of messages")
        else:
            logger.info(f"mllp: {source}: closing connection: mllp shutdown")
    client.close()

HL7_MSA_ACK_CODE_FIELD = 1
HL7_MSA_ACK_CODE_ACCEPT = b"AA"

def verify_ack(messages):
    if len(messages) != 1:
        return False, f"Expected 1 ack message, found {len(messages)}"
    segments =  messages[0].split(b"\r")
    segment_types = [s.split(b"|")[0] for s in segments]
    if b"MSH" not in segment_types:
        return False, "Expected MSH segment"
    if b"MSA" not in segment_types:
        return False, "Expected MSA segment"
    fields = segments[segment_types.index(b"MSA")].split(b"|")
    if len(fields) <= HL7_MSA_ACK_CODE_FIELD:
        return False, "Wrong number of fields in MSA segment"
    return fields[HL7_MSA_ACK_CODE_FIELD] == HL7_MSA_ACK_CODE_ACCEPT, None

def run_mllp_server(host, port, hl7_messages, shutdown_mllp, short_messages):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.settimeout(SHUTDOWN_POLL_INTERVAL_SECONDS)
        s.listen(1)
        logger.info(f"mllp: listening on {host}:{port}")
        while not shutdown_mllp.is_set():
            try:
                client, (host, port) = s.accept()
            except TimeoutError:
                continue
            source = f"{host}:{port}"
            logger.info(f"mllp: {source}: accepted connection")
            client.settimeout(MLLP_TIMEOUT_SECONDS)
            t = threading.Thread(target=serve_mllp_client, args=(client, source, hl7_messages, shutdown_mllp, short_messages), daemon=True)
            t.start()
        logger.info("mllp: graceful shutdown")

MLLP_START_OF_BLOCK = 0x0b
MLLP_END_OF_BLOCK = 0x1c
MLLP_CARRIAGE_RETURN = 0x0d

def parse_mllp_messages(buffer, source):
    i = 0
    messages = []
    consumed = 0
    expect = MLLP_START_OF_BLOCK
    while i < len(buffer):
        if expect is not None:
            if buffer[i] != expect:
                raise Exception(f"{source}: bad MLLP encoding: want {hex(expect)}, found {hex(buffer[i])}")
            if expect == MLLP_START_OF_BLOCK:
                expect = None
                consumed = i
            elif expect == MLLP_CARRIAGE_RETURN:
                messages.append(buffer[consumed+1:i-1])
                expect = MLLP_START_OF_BLOCK
                consumed = i + 1
        else:
            if buffer[i] == MLLP_END_OF_BLOCK:
                expect = MLLP_CARRIAGE_RETURN
        i += 1
    return messages, buffer[consumed:]

def read_hl7_messages(filename):
    with open(filename, "rb") as r:
        messages, remaining = parse_mllp_messages(r.read(), filename)
        if len(remaining) > 0:
                logger.info(f"messages: {len(messages)} remaining: {len(remaining)}")
                raise Exception(f"{filename}: Unexpected data at end of file")
        return messages

class PagerRequestHandler(http.server.BaseHTTPRequestHandler):

    def __init__(self, shutdown, *args, **kwargs):
        self.shutdown = shutdown
        super().__init__(*args, **kwargs)

    def do_POST(self):
        self.server_version = f"coursework3-simulator/{VERSION}"
        if self.path == "/page":
            self.do_POST_page()
        elif self.path == "/healthy":
            self.do_POST_healthy()
        elif self.path == "/shutdown":
            self.do_POST_shutdown()
        else:
            logger.error("pager: bad request: not /page")
            self.send_response(http.HTTPStatus.BAD_REQUEST)
            self.end_headers()

    def do_GET(self):
        self.do_POST()

    def do_POST_page(self):
        length = 0
        try:
            length = int(self.headers["Content-Length"])
        except Exception:
            logger.error("pager: bad request: no Content-Length")
            self.send_response(http.HTTPStatus.BAD_REQUEST, "No Content-Length")
            self.end_headers()
            return
        error = None
        mrn = None
        timestamp = None
        parts = str(self.rfile.read(length), "ascii").split(",")
        if len(parts) < 3:
            mrn = 0
            try:
                mrn = int(parts[0])
            except:
                error = "bad MRN in body"
            if not error and len(parts) == 2:
                try:
                    timestamp = datetime.datetime.strptime(parts[1], "%Y%m%d%H%M%S")
                except:
                    error = "bad timestamp in body"
        else:
            error = "expected at most two values: mrn,timestamp"
        if error:
            logger.error("pager: " + error)
            self.send_response(http.HTTPStatus.BAD_REQUEST, error)
            self.end_headers()
            return
        if timestamp:
            logger.info(f"pager: paging for MRN {mrn} at {timestamp}")
        else:
            logger.info(f"pager: paging for MRN {mrn}")
        self.send_response(http.HTTPStatus.OK)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"ok\n")

    def do_POST_healthy(self):
        self.send_response(http.HTTPStatus.OK)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"ok\n")

    def do_POST_shutdown(self):
        self.send_response(http.HTTPStatus.OK)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"ok\n")
        self.shutdown()

    def log_message(*args):
        pass # Prevent default logging

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--messages", default="messages.mllp", help="HL7 messages to replay, in MLLP format")
    parser.add_argument("--mllp", default=8440, type=int, help="Port on which to replay HL7 messages via MLLP")
    parser.add_argument("--pager", default=8441, type=int, help="Post on which to listen for pager requests via HTTP")
    parser.add_argument("--short_messages", default=False, action="store_true", help="Encourage all outgoing messages to be split in two")
    parser.add_argument("--no_log", default=False, action="store_true", help="Disable logging")
    flags = parser.parse_args()
    if flags.no_log:
        logger.disabled = True
    else:
        logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="{message}", style="{")
    hl7_messages = read_hl7_messages(flags.messages)
    shutdown_event = threading.Event()
    mllp_thread = threading.Thread(target=run_mllp_server, args=("0.0.0.0", flags.mllp, hl7_messages, shutdown_event, flags.short_messages), daemon=True)
    mllp_thread.start()
    pager = None
    def shutdown():
        shutdown_event.set()
        logger.info("pager: graceful shutdown")
        pager.shutdown()
    signal.signal(signal.SIGTERM, lambda signal, frame: shutdown())
    def new_pager_handler(*args, **kwargs):
        return PagerRequestHandler(shutdown, *args, **kwargs)
    pager = http.server.ThreadingHTTPServer(("0.0.0.0", flags.pager), new_pager_handler)
    logger.info(f"pager: listening on 0.0.0.0:{flags.pager}")
    pager_thread = threading.Thread(target=pager.serve_forever, args=(), kwargs={"poll_interval": SHUTDOWN_POLL_INTERVAL_SECONDS}, daemon=True)
    pager_thread.start()
    mllp_thread.join()
    pager_thread.join()

if __name__ == "__main__":
    main()