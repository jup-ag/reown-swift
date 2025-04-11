//     
import Foundation


public struct RelayClientFactory {

    public static func create(
        relayHost: String,
        projectId: String,
        socketFactory: WebSocketFactory,
        groupIdentifier: String,
        socketConnectionType: SocketConnectionType
    ) -> RelayClient {


        guard let keyValueStorage = UserDefaults(suiteName: groupIdentifier) else {
            fatalError("Could not instantiate UserDefaults for a group identifier \(groupIdentifier)")
        }
        let keychainStorage = KeychainStorage(serviceIdentifier: "com.walletconnect.sdk", accessGroup: groupIdentifier)

        let logger = ConsoleLogger(prefix: "🚄" ,loggingLevel: .off)

        let networkMonitor = NetworkMonitor()

        return RelayClientFactory.create(
            relayHost: relayHost,
            projectId: projectId,
            keyValueStorage: keyValueStorage,
            keychainStorage: keychainStorage,
            socketFactory: socketFactory,
            socketConnectionType: socketConnectionType,
            networkMonitor: networkMonitor,
            logger: logger
        )
    }


    public static func create(
        relayHost: String,
        projectId: String,
        keyValueStorage: KeyValueStorage,
        keychainStorage: KeychainStorageProtocol,
        socketFactory: WebSocketFactory,
        socketConnectionType: SocketConnectionType = .automatic,
        networkMonitor: NetworkMonitoring,
        logger: ConsoleLogging
    ) -> RelayClient {

        let clientIdStorage = ClientIdStorage(defaults: keyValueStorage, keychain: keychainStorage, logger: logger)

        let socketAuthenticator = ClientIdAuthenticator(
            clientIdStorage: clientIdStorage,
            logger: logger
        )
        let relayUrlFactory = RelayUrlFactory(
            relayHost: relayHost,
            projectId: projectId
        )
        let bundleId = Bundle.main.bundleIdentifier
        let socket = socketFactory.create(with: relayUrlFactory.create(bundleId: bundleId))

        socket.request.addValue(EnvironmentInfo.userAgent, forHTTPHeaderField: "User-Agent")

        do {
            let authToken = try socketAuthenticator.createAuthToken(url: "wss://" + relayHost)
            socket.request.setValue("Bearer \(authToken)", forHTTPHeaderField: "Authorization")
        } catch {
            print("Auth token creation error: \(error.localizedDescription)")
        }

        let subscriptionsTracker = SubscriptionsTracker(logger: logger)
        let topicsTracker = TopicsTracker()

        let socketStatusProvider = SocketStatusProvider(socket: socket, logger: logger)
        var socketConnectionHandler: SocketConnectionHandler!
        switch socketConnectionType {
        case .automatic:    socketConnectionHandler = AutomaticSocketConnectionHandler(socket: socket, subscriptionsTracker: subscriptionsTracker, logger: logger, socketStatusProvider: socketStatusProvider, clientIdAuthenticator: socketAuthenticator)
        case .manual:       socketConnectionHandler = ManualSocketConnectionHandler(socket: socket, logger: logger, topicsTracker: topicsTracker, clientIdAuthenticator: socketAuthenticator, socketStatusProvider: socketStatusProvider)
        }

        let dispatcher = Dispatcher(
            socketFactory: socketFactory,
            relayUrlFactory: relayUrlFactory,
            networkMonitor: networkMonitor,
            socket: socket,
            logger: logger,
            socketConnectionHandler: socketConnectionHandler,
            socketStatusProvider: socketStatusProvider
        )

        let rpcHistory = RPCHistoryFactory.createForRelay(keyValueStorage: keyValueStorage)

        return RelayClient(dispatcher: dispatcher, logger: logger, rpcHistory: rpcHistory, clientIdStorage: clientIdStorage, subscriptionsTracker: subscriptionsTracker, topicsTracker: topicsTracker)
    }
}
