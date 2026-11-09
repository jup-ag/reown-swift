import Foundation
import Combine
import JSONRPC
import WalletConnectKMS
import WalletConnectNetworking
@testable import WalletConnectRelay
@testable import WalletConnectUtils
@testable import TestingUtils

/// The real networking and relay client over a socket that takes every message and acknowledges none,
/// as when the relay's acknowledgement is lost, so every publish times out: after a millisecond here,
/// 60 s in an app. Requests and responses are recorded in, validated against and resolved in `history`.
func networkingWhoseRelayNeverAcknowledges(history: RPCHistory) -> NetworkingInteractor {
    let relayClient = RelayClient(
        dispatcher: SocketWithoutAcknowledgements(),
        logger: ConsoleLoggerMock(),
        rpcHistory: RPCHistoryFactory.createForRelay(keyValueStorage: RuntimeKeyValueStorage()),
        clientIdStorage: NoClientId(),
        subscriptionsTracker: SubscriptionsTrackerMock(),
        acknowledgementTimeout: .milliseconds(1)
    )
    return NetworkingInteractor(
        relayClient: relayClient,
        serializer: PlainSerializer(),
        logger: ConsoleLoggerMock(),
        rpcHistory: history
    )
}

/// A relay socket that takes every message and acknowledges none.
private final class SocketWithoutAcknowledgements: Dispatching {
    var onMessage: ((String) -> Void)?
    var isSocketConnected: Bool { true }
    var networkConnectionStatusPublisher: AnyPublisher<NetworkConnectionStatus, Never> {
        Just(.connected).eraseToAnyPublisher()
    }
    var socketConnectionStatusPublisher: AnyPublisher<SocketConnectionStatus, Never> {
        Empty(completeImmediately: false).eraseToAnyPublisher()
    }

    func protectedSend(_ string: String, completion: @escaping (Error?) -> Void) {
        completion(nil)
    }

    func protectedSend(_ string: String) async throws {}

    func connect() throws {}

    func disconnect(closeCode: URLSessionWebSocketTask.CloseCode) throws {}
}

/// Sends every message as is: what goes over the socket is not under test.
private struct PlainSerializer: Serializing {
    struct NotADecoder: Error {}

    var logsPublisher: AnyPublisher<Log, Never> { Empty().eraseToAnyPublisher() }
    var logger: ConsoleLogging { ConsoleLoggerMock() }

    func setLogging(level: LoggingLevel) {}

    func serialize(topic: String?, encodable: Encodable, envelopeType: Envelope.EnvelopeType, codingType: Envelope.CodingType) throws -> String {
        "envelope"
    }

    func deserialize<T: Codable>(topic: String, codingType: Envelope.CodingType, envelopeString: String) throws -> (T, derivedTopic: String?, decryptedPayload: Data) {
        throw NotADecoder()
    }
}

/// Publishing never asks for the client id.
private struct NoClientId: ClientIdStoring {
    struct NotStored: Error {}

    func getOrCreateKeyPair() throws -> SigningPrivateKey { throw NotStored() }
    func getClientId() throws -> String { throw NotStored() }
}
