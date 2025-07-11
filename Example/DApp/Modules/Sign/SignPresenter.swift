import UIKit
import Combine

import ReownAppKit
import WalletConnectSign

final class SignPresenter: ObservableObject {
    @Published var accountsDetails = [AccountDetails]()
    
    @Published var showError = false
    @Published var errorMessage = String.empty
    
    var walletConnectUri: WalletConnectURI?
    
    let chains = [
        Chain(name: "Ethereum", id: "eip155:1"),
        Chain(name: "Polygon", id: "eip155:137"),
        Chain(name: "Solana", id: "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp")
    ]
    
    private let interactor: SignInteractor
    private let router: SignRouter

    private var session: Session?
    
    private var subscriptions = Set<AnyCancellable>()

    init(
        interactor: SignInteractor,
        router: SignRouter
    ) {
        defer { setupInitialState() }
        self.interactor = interactor
        self.router = router
    }
    
    func onAppear() {
        
    }
    
    func copyUri() {
        UIPasteboard.general.string = walletConnectUri?.absoluteString
    }
    
    func connectWalletWithW3M() {
        Task {
            AppKit.set(sessionParams: .init(
                namespaces: Proposal.namespaces
            ))
        }
        AppKit.present(from: nil)
    }

    @MainActor
    func connectWalletWithSessionPropose() {
        Task {
            do {
                ActivityIndicatorManager.shared.start()
                walletConnectUri = try await Sign.instance.connect(
                    namespaces: Proposal.namespaces
                )
                ActivityIndicatorManager.shared.stop()
                router.presentNewPairing(walletConnectUri: walletConnectUri!)
            } catch {
                ActivityIndicatorManager.shared.stop()
            }
        }
    }

    @MainActor
    func connectWalletWithSessionAuthenticate() {
        Task {
            do {
                ActivityIndicatorManager.shared.start()
                let uri = try await Sign.instance.authenticate(.stub())
                walletConnectUri = uri
                ActivityIndicatorManager.shared.stop()
                router.presentNewPairing(walletConnectUri: walletConnectUri!)
            } catch {
                ActivityIndicatorManager.shared.stop()
            }
        }
    }

    @MainActor
    func connectWalletWithSessionAuthenticateSIWEOnly() {
        Task {
            do {
                ActivityIndicatorManager.shared.start()
                let uri = try await Sign.instance.authenticate(.stub(methods: ["personal_sign"]))
                walletConnectUri = uri
                ActivityIndicatorManager.shared.stop()
                router.presentNewPairing(walletConnectUri: walletConnectUri!)
            } catch {
                ActivityIndicatorManager.shared.stop()
            }
        }
    }

    @MainActor
    func connectWalletWithSessionAuthenticateLinkMode() {
        Task {
            do {
                ActivityIndicatorManager.shared.start()
                if let pairingUri = try await Sign.instance.authenticate(.stub(methods: ["personal_sign"]), walletUniversalLink: "https://lab.web3modal.com/wallet") {
                    walletConnectUri = pairingUri
                    ActivityIndicatorManager.shared.stop()
                    router.presentNewPairing(walletConnectUri: walletConnectUri!)
                }
            } catch {
                AlertPresenter.present(message: error.localizedDescription, type: .error)
                ActivityIndicatorManager.shared.stop()
            }
        }
    }

    @MainActor
    func openConfiguration() {
        router.openConfig()
    }

    @MainActor
    func disconnect() {
        if let session {
            Task { @MainActor in
                do {
                    ActivityIndicatorManager.shared.start()
                    try await Sign.instance.disconnect(topic: session.topic)
                    ActivityIndicatorManager.shared.stop()
                    accountsDetails.removeAll()
                } catch {
                    ActivityIndicatorManager.shared.stop()
                    showError.toggle()
                    errorMessage = error.localizedDescription
                }
            }
        }
    }
    
    func presentSessionAccount(sessionAccount: AccountDetails) {
        if let session {
            router.presentSessionAccount(sessionAccount: sessionAccount, session: session)
        }
    }
}

// MARK: - Private functions
extension SignPresenter {
    private func setupInitialState() {
        getSession()
        
        Sign.instance.sessionDeletePublisher
            .receive(on: DispatchQueue.main)
            .sink { [unowned self] _ in
                self.accountsDetails.removeAll()
                router.popToRoot()
                Task(priority: .high) { ActivityIndicatorManager.shared.stop() }
            }
            .store(in: &subscriptions)

        Sign.instance.sessionSettlePublisher
            .receive(on: DispatchQueue.main)
            .sink { [unowned self] _ in
                self.getSession()
            }
            .store(in: &subscriptions)

        Sign.instance.authResponsePublisher
            .receive(on: DispatchQueue.main)
            .sink { [unowned self] response in
                switch response.result {
                case .success(let (session, _)):
                    if session == nil {
                        AlertPresenter.present(message: "Wallet Succesfully Authenticated", type: .success)
                    } else {
                        self.router.dismiss()
                        self.getSession()
                    }
                    break
                case .failure(let error):
                    AlertPresenter.present(message: error.localizedDescription, type: .error)
                }
                Task(priority: .high) { ActivityIndicatorManager.shared.stop() }
            }
            .store(in: &subscriptions)

        Sign.instance.sessionResponsePublisher
            .receive(on: DispatchQueue.main)
            .sink { response in
                Task(priority: .high) { ActivityIndicatorManager.shared.stop() }
            }
            .store(in: &subscriptions)

        Sign.instance.requestExpirationPublisher
            .receive(on: DispatchQueue.main)
            .sink { _ in
                Task(priority: .high) { ActivityIndicatorManager.shared.stop() }
                AlertPresenter.present(message: "Session Request has expired", type: .warning)
            }
            .store(in: &subscriptions)

        AppKit.instance.SIWEAuthenticationPublisher
            .receive(on: DispatchQueue.main)
            .sink { [unowned self] result in
                switch result {
                case .success((let message, let signature)):
                    AlertPresenter.present(message: "Authenticated with SIWE", type: .success)
                    self.router.dismiss()
                    self.getSession()
                case .failure(let error):
                    AlertPresenter.present(message: "\(error)", type: .warning)
                }
            }
            .store(in: &subscriptions)
    }
    
    private func getSession() {
        if let session = Sign.instance.getSessions().first {
            self.session = session
            session.namespaces.values.forEach { namespace in
                namespace.accounts.forEach { account in
                    accountsDetails.append(
                        AccountDetails(
                            chain: account.blockchainIdentifier,
                            methods: Array(namespace.methods),
                            address: account.address
                        )
                    )
                }
            }
        }
    }
}

// MARK: - SceneViewModel
extension SignPresenter: SceneViewModel {}


// MARK: - Authenticate request stub
extension AuthRequestParams {
    static func stub(
        domain: String = "lab.web3modal.com",
        chains: [String] = ["eip155:1", "eip155:137"],
        nonce: String = "32891756",
        uri: String = "https://lab.web3modal.com",
        nbf: String? = nil,
        exp: String? = nil,
        statement: String? = "I accept the ServiceOrg Terms of Service: https://app.web3inbox.com/tos",
        requestId: String? = nil,
        resources: [String]? = nil,
        methods: [String]? = ["personal_sign", "eth_sendTransaction"]
    ) -> AuthRequestParams {
        return try! AuthRequestParams(
            domain: domain,
            chains: chains,
            nonce: nonce,
            uri: uri,
            nbf: nbf,
            exp: exp,
            statement: statement,
            requestId: requestId,
            resources: resources,
            methods: methods
        )
    }
}

