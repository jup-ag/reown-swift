import Foundation

public enum RelayError: Error, LocalizedError {
    /// The relay did not acknowledge a publish in time. The message went out, so its peer may have it.
    case requestTimeout

    public var errorDescription: String? {
        return localizedDescription
    }

    var localizedDescription: String {
        switch self {
        case .requestTimeout:
            return "Relay request timeout"
        }
    }
}
