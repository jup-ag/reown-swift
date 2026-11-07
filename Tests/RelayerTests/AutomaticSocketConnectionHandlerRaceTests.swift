import Combine
import Foundation
import XCTest
@testable import WalletConnectRelay

/// Stress test for concurrent access to `socket.request` from AutomaticSocketConnectionHandler.
/// The socket mirrors the app's `WalletKit.NativeWebSocket`: a plain stored `var request: URLRequest`
/// whose `didSet` builds a new URLSessionWebSocketTask.
private final class StressSocket: WebSocketConnecting {
    var isConnected = false
    var onConnect: (() -> Void)?
    var onDisconnect: ((Error?) -> Void)?
    var onText: ((String) -> Void)?
    var request: URLRequest {
        didSet { task = session.webSocketTask(with: request) }
    }
    private var task: URLSessionWebSocketTask?
    private let session = URLSession(configuration: .ephemeral)

    init() {
        var r = URLRequest(url: URL(string: "wss://relay.walletconnect.org/?auth=abc&projectId=123&ua=jupiter")!)
        r.setValue("Bearer initial-token", forHTTPHeaderField: "Authorization")
        r.setValue("https://jup.ag", forHTTPHeaderField: "Origin")
        request = r
        task = session.webSocketTask(with: r)
    }
    func connect() { /* like NativeWebSocket: task?.resume() (omitted: no network in test) */ _ = task }
    func disconnect() {}
    func write(string: String, completion: (() -> Void)?) { completion?() }
}

private final class StressAuthenticator: ClientIdAuthenticating {
    private let counter = NSLock()
    private var n = 0
    func createAuthToken(url: String) throws -> String { "created" }
    func refreshTokenIfNeeded(token: String, url: String) throws -> String {
        counter.lock(); n += 1; let v = n; counter.unlock()
        return "tok-\(v)-" + String(repeating: "x", count: 200)
    }
}

final class AutomaticSocketConnectionHandlerRaceTests: XCTestCase {

    func testConcurrentConnectsAndStatusFlapsDoNotRaceOnSocketRequest() async throws {
        let iterations = Int(ProcessInfo.processInfo.environment["RACE_ITERATIONS"] ?? "") ?? 3000
        let socket = StressSocket()
        let status = SocketStatusProviderMock()
        let network = NetworkMonitoringMock()
        let appState = AppStateObserverMock()
        let tracker = SubscriptionsTrackerMock()
        tracker.isSubscribedReturnValue = true
        let sut = AutomaticSocketConnectionHandler(
            socket: socket,
            networkMonitor: network,
            appStateObserver: appState,
            backgroundTaskRegistrar: BackgroundTaskRegistrarMock(),
            subscriptionsTracker: tracker,
            logger: ConsoleLoggerMock(),
            socketStatusProvider: status,
            clientIdAuthenticator: StressAuthenticator()
        )
        sut.requestTimeout = 0.02
        sut.periodicReconnectionInterval = 0.001

        let emitter = DispatchQueue(label: "stress.status", attributes: .concurrent)
        let group = DispatchGroup()

        await withTaskGroup(of: Void.self) { tg in
            // Several "subscribers" calling handleInternalConnect from the cooperative pool.
            for _ in 0..<8 {
                tg.addTask {
                    for _ in 0..<(iterations / 8) {
                        try? await sut.handleInternalConnect()
                        await Task.yield()
                    }
                }
            }
            // Status flapping (stale .disconnected from previous socket, then .connected) from URLSession-like threads.
            for t in 0..<2 {
                tg.addTask {
                    for i in 0..<iterations {
                        group.enter()
                        emitter.async {
                            status.simulateConnectionStatus((i + t) % 3 == 0 ? .connected : .disconnected)
                            group.leave()
                        }
                        if i % 16 == 0 { await Task.yield() }
                    }
                }
            }
            // Network-monitor and foreground reconnects, plus explicit connect().
            tg.addTask {
                for i in 0..<iterations {
                    network.networkConnectionStatusPublisherSubject.send(.connected)
                    appState.onWillEnterForeground?()
                    sut.connect()
                    sut.reconnectIfNeeded()
                    if i % 16 == 0 { await Task.yield() }
                }
            }
        }
        group.wait()
        // drain syncQueue
        sut.syncQueue.sync {}
        XCTAssertNotNil(socket.request.url)
    }
}
