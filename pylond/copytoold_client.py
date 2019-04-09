# Copyright (c) 2019 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Client to send request to copytool manager daemon
"""
import time
import zmq

from pylond import copytoold_pb2


COPYTOOLD_CLIENT_POLL_TIMEOUT = 1
COPYTOOLD_CLIENT_TIMEOUT = 10


class CopytooldClientMessage(object):
    """
    Each message has a object of this type
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, request_type, reply_type):
        self.ccm_request = copytoold_pb2.CopytooldMessage()
        self.ccm_request.cm_protocol_version = copytoold_pb2.CopytooldMessage.CPV_ZERO
        self.ccm_request.cm_type = request_type
        self.ccm_request.cm_errno = copytoold_pb2.CopytooldMessage.CE_NO_ERROR
        self.ccm_reply = copytoold_pb2.CopytooldMessage()
        self.ccm_reply_type = reply_type

    def ccm_communicate(self, log, poll, socket_client, timeout):
        """
        Send the request and wait for the reply
        """
        # pylint: disable=too-many-return-statements
        # If communicate failed because of un-recoverable error, return
        # negative value. If times out, return 1.
        log.cl_debug("communicating to server")
        request_string = self.ccm_request.SerializeToString()
        socket_client.send(request_string)
        received = False
        time_start = time.time()
        log.cl_debug("sent request to server")
        while not received:
            time_now = time.time()
            elapsed = time_now - time_start
            if elapsed >= timeout:
                log.cl_error("timeout after waiting for [%d] seconds when "
                             "communcating to server", timeout)
                return -1

            events = dict(poll.poll(COPYTOOLD_CLIENT_POLL_TIMEOUT * 1000))
            for socket, event in events.iteritems():
                if socket != socket_client:
                    log.cl_error("found a event which doesn't belong to this "
                                 "socket, ignoring")
                    continue
                if event == zmq.POLLIN:
                    log.cl_debug("received the reply successfully")
                    received = True

        log.cl_debug("received the reply from server")
        reply_string = socket_client.recv()
        if not reply_string:
            log.cl_error("got POLLIN event, but no message received")
            return -1

        log.cl_debug("parsing the reply from server")
        self.ccm_reply.ParseFromString(reply_string)
        if (self.ccm_reply.cm_protocol_version !=
                copytoold_pb2.CopytooldMessage.CPV_ZERO):
            log.cl_error("wrong reply protocol version [%d], expected [%d]",
                         self.ccm_reply.cm_protocol_version,
                         copytoold_pb2.CopytooldMessage.CPV_ZERO)
            return -1

        if self.ccm_reply.cm_type != self.ccm_reply_type:
            log.cl_error("wrong reply type [%d], expected [%d]",
                         self.ccm_reply.cm_type,
                         self.ccm_reply_type)
            return -1

        if self.ccm_reply.cm_errno != copytoold_pb2.CopytooldMessage.CE_NO_ERROR:
            log.cl_error("server side error [%d]", self.ccm_reply.cm_errno)
            return -1

        if self.ccm_reply.cm_type == copytoold_pb2.CopytooldMessage.CMT_START_REPLY:
            log.cl_debug("got connect reply from server")
            return 0

        log.cl_debug("communicated successfully with server")
        return 0


class CopytooldClient(object):
    """
    Client of Copytoold
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, server_url):
        self.cc_context = zmq.Context(1)
        self.cc_poll = zmq.Poller()
        self.cc_server_url = server_url
        self.cc_client = self.cc_context.socket(zmq.REQ)
        self.cc_client.connect(self.cc_server_url)
        self.cc_poll.register(self.cc_client, zmq.POLLIN)

    def cc_send_start_request(self, log, source, dest):
        """
        Send a start request to Copytoold
        """
        message = CopytooldClientMessage(copytoold_pb2.CopytooldMessage.CMT_START_REQUEST,
                                         copytoold_pb2.CopytooldMessage.CMT_START_REPLY)
        message.ccm_request.cm_start_request.csr_archive_id_number = 0
        message.ccm_request.cm_start_request.csr_source = source
        message.ccm_request.cm_start_request.csr_dest = dest
        ret = message.ccm_communicate(log, self.cc_poll, self.cc_client,
                                      COPYTOOLD_CLIENT_TIMEOUT)
        if ret:
            log.cl_stderr("failed to send start request")
        return ret

    def cc_fini(self):
        """
        Finish the connection to the server
        """
        self.cc_client.setsockopt(zmq.LINGER, 0)
        self.cc_client.close()
        self.cc_poll.unregister(self.cc_client)
        self.cc_context.term()
