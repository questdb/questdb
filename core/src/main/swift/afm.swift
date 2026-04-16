//**************************************************************************
//     ___                  _   ____  ____
//    / _ \ _   _  ___  ___| |_|  _ \| __ )
//   | | | | | | |/ _ \/ __| __| | | |  _ \
//   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
//    \__\_\\__,_|\___||___/\__|____/|____/
//
//  Copyright (c) 2014-2019 Appsicle
//  Copyright (c) 2019-2026 QuestDB
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
//**************************************************************************

import Foundation
#if canImport(FoundationModels)
import FoundationModels
#endif

private final class ResultHolder: @unchecked Sendable {
    var content: String?
    var errorMessage: String?
}

private func cstr(_ s: String) -> UnsafeMutablePointer<CChar>? {
    return s.withCString { strdup($0) }
}

private func setError(_ out: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?, _ msg: String) {
    out?.pointee = cstr(msg)
}

@_cdecl("afm_generate")
public func afm_generate(
    _ prompt: UnsafePointer<CChar>?,
    _ errorOut: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
) -> UnsafeMutablePointer<CChar>? {
    guard let prompt = prompt else {
        setError(errorOut, "prompt is null")
        return nil
    }
    let promptStr = String(cString: prompt)

#if canImport(FoundationModels)
    if #available(macOS 26.0, *) {
        let model = SystemLanguageModel.default
        switch model.availability {
        case .available:
            break
        case .unavailable(let reason):
            setError(errorOut, "Apple Foundation Model unavailable: \(reason)")
            return nil
        @unknown default:
            setError(errorOut, "Apple Foundation Model unavailable: unknown reason")
            return nil
        }

        let holder = ResultHolder()
        let semaphore = DispatchSemaphore(value: 0)

        Task.detached {
            do {
                let session = LanguageModelSession()
                let response = try await session.respond(to: promptStr)
                holder.content = response.content
            } catch {
                holder.errorMessage = "model call failed: \(error.localizedDescription)"
            }
            semaphore.signal()
        }
        semaphore.wait()

        if let content = holder.content {
            return cstr(content)
        }
        setError(errorOut, holder.errorMessage ?? "unknown error")
        return nil
    } else {
        setError(errorOut, "Apple Foundation Models requires macOS 26.0 or later")
        return nil
    }
#else
    setError(errorOut, "FoundationModels framework not available at build time")
    return nil
#endif
}

@_cdecl("afm_free")
public func afm_free(_ ptr: UnsafeMutablePointer<CChar>?) {
    if let p = ptr { free(p) }
}

// Streaming path: pull-based handle. Java calls start/next/close.

#if canImport(FoundationModels)
@available(macOS 26.0, *)
private final class StreamState: @unchecked Sendable {
    let condition = NSCondition()
    var tokens: [String] = []
    var done = false
    var errorMessage: String? = nil
    var task: Task<Void, Never>? = nil

    func push(_ delta: String) {
        condition.lock()
        tokens.append(delta)
        condition.broadcast()
        condition.unlock()
    }

    func finish(error: String? = nil) {
        condition.lock()
        if error != nil { errorMessage = error }
        done = true
        condition.broadcast()
        condition.unlock()
    }

    // Returns (token, error, isEnd). token != nil → emit. token == nil && isEnd → stream ended.
    // error is set on terminal error.
    func nextBlocking() -> (token: String?, error: String?, isEnd: Bool) {
        condition.lock()
        defer { condition.unlock() }
        while tokens.isEmpty && !done {
            condition.wait()
        }
        if !tokens.isEmpty {
            return (tokens.removeFirst(), nil, false)
        }
        return (nil, errorMessage, true)
    }

    func cancel() {
        task?.cancel()
        finish()
    }
}
#endif

@_cdecl("afm_stream_start")
public func afm_stream_start(
    _ prompt: UnsafePointer<CChar>?,
    _ errorOut: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
) -> UnsafeMutableRawPointer? {
    guard let prompt = prompt else {
        setError(errorOut, "prompt is null")
        return nil
    }
    let promptStr = String(cString: prompt)

#if canImport(FoundationModels)
    if #available(macOS 26.0, *) {
        let model = SystemLanguageModel.default
        switch model.availability {
        case .available:
            break
        case .unavailable(let reason):
            setError(errorOut, "Apple Foundation Model unavailable: \(reason)")
            return nil
        @unknown default:
            setError(errorOut, "Apple Foundation Model unavailable: unknown reason")
            return nil
        }

        let state = StreamState()
        state.task = Task.detached {
            do {
                let session = LanguageModelSession()
                let stream = session.streamResponse(to: promptStr)
                var previous = ""
                for try await snapshot in stream {
                    let current = snapshot.content
                    if current.count > previous.count {
                        let start = current.index(current.startIndex, offsetBy: previous.count)
                        let delta = String(current[start...])
                        state.push(delta)
                        previous = current
                    }
                }
                state.finish()
            } catch {
                state.finish(error: "streaming failed: \(error.localizedDescription)")
            }
        }
        return UnsafeMutableRawPointer(Unmanaged.passRetained(state).toOpaque())
    } else {
        setError(errorOut, "Apple Foundation Models requires macOS 26.0 or later")
        return nil
    }
#else
    setError(errorOut, "FoundationModels framework not available at build time")
    return nil
#endif
}

@_cdecl("afm_stream_next")
public func afm_stream_next(
    _ handle: UnsafeMutableRawPointer?,
    _ errorOut: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
) -> UnsafeMutablePointer<CChar>? {
    guard let handle = handle else {
        setError(errorOut, "handle is null")
        return nil
    }

#if canImport(FoundationModels)
    if #available(macOS 26.0, *) {
        let state = Unmanaged<StreamState>.fromOpaque(handle).takeUnretainedValue()
        let (token, err, _) = state.nextBlocking()
        if let token = token {
            return cstr(token)
        }
        if let err = err {
            setError(errorOut, err)
        }
        return nil
    }
#endif
    setError(errorOut, "Apple Foundation Models not available")
    return nil
}

@_cdecl("afm_stream_close")
public func afm_stream_close(_ handle: UnsafeMutableRawPointer?) {
    guard let handle = handle else { return }
#if canImport(FoundationModels)
    if #available(macOS 26.0, *) {
        let unmanaged = Unmanaged<StreamState>.fromOpaque(handle)
        unmanaged.takeUnretainedValue().cancel()
        unmanaged.release()
    }
#endif
}
