import UIKit
import Combine
import ReownWalletKit
import Foundation

final class CATransactionPresenter: ObservableObject {
    enum Errors: LocalizedError {
        case invalidURL
        case invalidResponse
        case invalidData
        case noSolanaAccountFound
    }

    // Published properties to be used in the view
    @Published var payingAmount: String = ""
    @Published var balanceAmount: String = ""
    @Published var appURL: String = ""
    @Published var networkName: String!
    @Published var estimatedFees: String = ""
    @Published var bridgeFee: String = ""
    @Published var executionSpeed: String = "Fast (~20 sec)"
    @Published var transactionCompleted: Bool = false
    @Published var fundingFromNetwork: String!
    @Published var payingTokenSymbol: String = "USDC"
    @Published var payingTokenDecimals: Int = 6 // Default to USDC's 6 decimals
    
    // Formatted fees with proper decimal places
    @Published var formattedEstimatedFees: String = ""
    @Published var formattedBridgeFee: String = ""

    private let sessionRequest: Request?
    var fundingFrom: [FundingMetadata] {
        return uiFields.routeResponse.metadata.fundingFrom
    }
    var initialTransactionMetadata: InitialTransactionMetadata {
        return uiFields.routeResponse.metadata.initialTransaction
    }
    let router: CATransactionRouter
    let importAccount: ImportAccount
    var uiFields: UiFields
    let call: Call
    var chainId: Blockchain
    let from: String

    private var disposeBag = Set<AnyCancellable>()

    init(
        sessionRequest: Request?,
        importAccount: ImportAccount,
        router: CATransactionRouter,
        call: Call,
        from: String,
        chainId: Blockchain,
        uiFields: UiFields
    ) {
        self.sessionRequest = sessionRequest
        self.router = router
        self.importAccount = importAccount
        self.chainId = chainId
        self.call = call
        self.from = from
        self.uiFields = uiFields
        self.networkName = network(for: chainId.absoluteString)
        self.fundingFromNetwork = network(for: fundingFrom[0].chainId)
        
        // Determine token type based on the contract address (if available)
        let tokenAddress = call.to.lowercased()
        // Check if the token is USDS based on the contract addresses
        if isUSDSToken(tokenAddress, chainId: chainId.absoluteString) {
            self.payingTokenSymbol = "USDS"
            self.payingTokenDecimals = 18
        } else {
            // Default to USDC with 6 decimals
            self.payingTokenSymbol = "USDC"
            self.payingTokenDecimals = 6
        }

        setupInitialState()
    }
    
    // Helper method to determine if a token is USDS based on its address
    private func isUSDSToken(_ tokenAddress: String, chainId: String) -> Bool {
        // Get the blockchain from the chainId string
        if let blockchain = Blockchain(chainId) {
            // Solana doesn't support DAI
            if blockchain.namespace == "solana" {
                return false
            }
            
            // Find which L2 network this corresponds to
            let l2Networks: [L2] = [.Arbitrium, .Optimism, .Base, .Solana]
            
            for network in l2Networks {
                if network.chainId == blockchain {
                    // Check if this token address matches the USDS address for this network
                    return tokenAddress.lowercased() == network.usdsContractAddress.lowercased()
                }
            }
        }
        
        return false
    }

    // MARK: - Fee Formatting Methods
    
    /// Formats a fee amount string to display with proper decimal places based on token type
    func formatFeeAmount(_ feeString: String) -> String {
        // Check if the fee string has a currency symbol and extract the number part
        if let numberPart = extractAmountFromFormattedString(feeString),
           let feeValue = Double(numberPart) {
            
            // The fees are displayed in USD, but we want to adjust the precision based on the token's decimals
            // For USDC/USDT with 6 decimals, standard 2 decimal places is fine
            // For USDS with 18 decimals, we might want to show more precision
            let formattedValue: String
            
            if payingTokenDecimals == 18 {
                // For USDS (18 decimals), show more precision if needed
                formattedValue = String(format: "%.4f", feeValue)
            } else {
                // For USDC/USDT (6 decimals), standard 2 decimal places
                formattedValue = String(format: "%.2f", feeValue)
            }
            
            return "$\(formattedValue)"
        }
        
        // Return the original string if parsing fails
        return feeString
    }
    
    /// Extracts the numeric part from a formatted fee string (e.g. "$1.23" -> "1.23")
    private func extractAmountFromFormattedString(_ formattedString: String) -> String? {
        // Find the numeric part (assume it's after a currency symbol)
        // This is a simple extraction - handles strings like "$1.23"
        if let currencyIndex = formattedString.firstIndex(of: "$") {
            let numberPart = formattedString[formattedString.index(after: currencyIndex)...]
            return String(numberPart).trimmingCharacters(in: .whitespaces)
        }
        return nil
    }

    func dismiss() {
        router.dismiss()
    }

    func approveTransactions() async throws -> ExecuteDetails {
        do {
            print("🚀 Starting transaction approval process...")
            ActivityIndicatorManager.shared.start()

            let initialTxHash = uiFields.initial.transactionHashToSign

            var routeTxnSigs = [RouteSig]()
            let signer = ETHSigner(importAccount: importAccount)

            print("📝 Signing route transactions...")
            for route in uiFields.route {
                switch route {
                case .eip155(let txnDetails):
                    var eip155Sigs = [String]()
                    for txnDetail in txnDetails {
                        print("EVM transaction detected")
                        let hash = txnDetail.transactionHashToSign
                            // sign with sol signer
                        let sig = try! signer.signHash(hash)
                        eip155Sigs.append(sig)
                        print("🔑 Signed transaction hash: \(hash)")
                    }
                    routeTxnSigs.append(.eip155(eip155Sigs))
                case .solana(let solanaTxnDetails):
                    var solanaSigs = [String]()
                    guard let privateKey = SolanaAccountStorage().getPrivateKey() else {
                        throw Errors.noSolanaAccountFound
                    }
                    for txnDetail in solanaTxnDetails {
                        print("Solana transaction detected")

                        let hash = txnDetail.transactionHashToSign

                        let signature = solanaSignPrehash(keypair: privateKey, message: hash)

                        solanaSigs.append(signature)
                        print("🔑 Signed transaction hash: \(hash)")
                    }
                    routeTxnSigs.append(.solana(solanaSigs))
                }
            }

            let initialTxnSig = try! signer.signHash(initialTxHash)
            print("🔑 Signed initial transaction hash: \(initialTxHash)")

            print("📝 Executing transactions through WalletKit...")
            let executeDetails = try await WalletKit.instance.ChainAbstraction.execute(uiFields: uiFields, routeTxnSigs: routeTxnSigs, initialTxnSig: initialTxnSig)

            print("✅ Transaction approval process completed successfully.")
            AlertPresenter.present(message: "Transaction approved successfully", type: .success)
            if let sessionRequest = sessionRequest {
                try await WalletKit.instance.respond(topic: sessionRequest.topic, requestId: sessionRequest.id, response: .response(AnyCodable(executeDetails.initialTxnHash)))
            }
            ActivityIndicatorManager.shared.stop()
            await MainActor.run {
                transactionCompleted = true
            }
            return executeDetails
        } catch {
            print("❌ Transaction approval failed with error: \(error.localizedDescription)")
            ActivityIndicatorManager.shared.stop()
            throw error
        }
    }

    @MainActor
    func rejectTransactions() async throws {
        try await respondError()
    }

    func respondError() async throws {
        guard let sessionRequest = sessionRequest else { return }
        do {
            ActivityIndicatorManager.shared.start()
            try await WalletKit.instance.respond(
                topic: sessionRequest.topic,
                requestId: sessionRequest.id,
                response: .error(.init(code: 0, message: ""))
            )
            ActivityIndicatorManager.shared.stop()
            await MainActor.run {
                router.dismiss()
            }
        } catch {
            ActivityIndicatorManager.shared.stop()
            AlertPresenter.present(message: error.localizedDescription, type: .error)
        }
    }

    func network(for chainId: String) -> String {
        let chainIdToNetwork = [
            "eip155:10": "Optimism",
            "eip155:42161": "Arbitrium",
            "eip155:8453": "Base",
            "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp": "Solana"
        ]
        return chainIdToNetwork[chainId]!
    }

    // Updated to respect token decimals
    func hexAmountToDenominatedUSDC(_ hexAmount: String) -> String {
        guard let indecValue = hexToDecimal(hexAmount) else {
            return "Invalid amount"
        }
        
        // Use the appropriate divisor based on token decimals
        let divisor = pow(10.0, Double(payingTokenDecimals))
        let tokenValue = Double(indecValue) / divisor
        
        return String(format: "%.2f", tokenValue)
    }

    func hexToDecimal(_ hex: String) -> Int? {
        let cleanHex = hex.hasPrefix("0x") ? String(hex.dropFirst(2)) : hex
        return Int(cleanHex, radix: 16)
    }

    func setupInitialState() {
        // Get the original fee strings
        estimatedFees = uiFields.localTotal.formattedAlt
        bridgeFee = uiFields.bridge.first!.localFee.formattedAlt
        
        // Now format them with proper decimal places
        formattedEstimatedFees = formatFeeAmount(estimatedFees)
        formattedBridgeFee = formatFeeAmount(bridgeFee)

        if let session = WalletKit.instance.getSessions().first(where: { $0.topic == sessionRequest?.topic }) {
            self.appURL = session.peer.url
        }
        networkName = network(for: chainId.absoluteString)
        payingAmount = initialTransactionMetadata.amount

        let tx = call
        Task {
            let balance = try await WalletKit.instance.erc20Balance(chainId: chainId.absoluteString, token: tx.to, owner: importAccount.account.address)
            await MainActor.run {
                balanceAmount = balance
            }
        }
    }

    func onViewOnExplorer() {
        // Force unwrap the address from the import account
        let address = importAccount.account.address

        // Mapping of network names to Blockscout URLs
        let networkBaseURLMap: [String: String] = [
            "Optimism": "optimism.blockscout.com",
            "Arbitrium": "arbitrum.blockscout.com",
            "Base": "base.blockscout.com"
        ]

        // Force unwrap the base URL for the current network
        let baseURL = networkBaseURLMap[networkName]!

        // Construct the explorer URL
        let explorerURL = URL(string: "https://\(baseURL)/address/\(address)")!

        // Open the URL in Safari
        UIApplication.shared.open(explorerURL, options: [:], completionHandler: nil)
        print("🌐 Opened explorer URL: \(explorerURL)")
    }
}

// MARK: - SceneViewModel
extension CATransactionPresenter: SceneViewModel {}




