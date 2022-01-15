#pragma once

#include "log.h"
#include "protocol.h"

#include <deque>
#include <functional>
#include <memory>
#include <string>

#include <sys/socket.h>
#include <sys/types.h>

namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct SocketState
{
    int fd = 0;

    NProtocol::Message current_message;

    std::deque<std::string> output_queue;

    uint32_t current_output_sent_count = 0;
    std::string current_output;
};

using SocketStatePtr = std::shared_ptr<SocketState>;

////////////////////////////////////////////////////////////////////////////////

// input -> output func
using Handler =
    std::function<std::string(char message_type, const std::string& message)>;

////////////////////////////////////////////////////////////////////////////////

inline bool process_input(SocketState& state, const Handler& handler)
{
    bool success = true;

    char buf[512];
    int total_read = 0;
    while (true) {
        auto len = std::min(sizeof(buf), state.current_message.to_read());
        auto count = recv(state.fd, buf, len, 0);

        if (count == -1) {
            if (errno != EAGAIN) {
                // TODO proper logging
                perror("send failed");

                success = false;
            }

            break;
        } else if (count == 0) {
            break;
        }
        total_read += count;
        state.current_message.on_data(buf, count);

        if (count < len) {
            break;
        }

        if (state.current_message.is_complete()) {
            auto response = handler(
                state.current_message.message_type,
                state.current_message.buffer);

            state.current_message.reset();

            if (response.size()) {
                state.output_queue.push_back(std::move(response));
            }
        }
    }

    if (total_read == 0) {
        LOG_INFO("conn closed");
        success = false;
    }

    return success;
}

////////////////////////////////////////////////////////////////////////////////

inline bool process_output(SocketState& state)
{
    bool success = true;

    while (true) {
        auto& buffer = state.current_output;
        auto& offset = state.current_output_sent_count;

        if (state.current_output.empty()) {
            if (state.output_queue.empty()) {
                break;
            }

            buffer = state.output_queue.front();
            offset = 0;
            state.output_queue.pop_front();
        }

        auto len = buffer.size() - offset;

        const auto count = send(
            state.fd,
            buffer.data() + offset,
            len,
            MSG_NOSIGNAL);

        if (count == -1) {
            if (errno != EAGAIN) {
                // TODO proper logging
                perror("send failed");

                success = false;
            }

            break;
        } else if (count == 0) {
            break;
        }

        if (count == len) {
            buffer.clear();
        } else {
            offset += count;
        }
    }

    return success;
}

}   // namespace NRpc
