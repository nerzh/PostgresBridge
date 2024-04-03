import AsyncKit
import Bridges

public struct PostgresConnectionSource: BridgesPoolSource {
    public let db: DatabaseIdentifier

    public init(_ db: DatabaseIdentifier) {
        self.db = db
    }

    public func makeConnection(logger: Logger, on eventLoop: EventLoop) -> EventLoopFuture<PostgresConnection> {
        let address: SocketAddress
        do {
            address = try self.db.host.address()
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
        
        return PostgresConnection.connect(
            to: address,
            tlsConfiguration: self.db.host.tlsConfiguration,
            logger: logger,
            on: eventLoop
        ).flatMap { conn in
            #warning("Because the Swift out of mind")
            let fuckingSwiftAmbigous: @Sendable (Error) throws -> Void = { error in
                #warning("Because the Swift out of mind again")
                let future: EventLoopFuture<Void> = conn.close()
                _ = future
                throw error
            }
            
            return conn.authenticate(
                username: self.db.host.username,
                database: self.db.name,
                password: self.db.host.password,
                logger: logger
            ).flatMapErrorThrowing(fuckingSwiftAmbigous).map { conn }
        }
    }
    
    public func makeConnection(logger: Logger, on eventLoop: EventLoop) async throws -> PostgresConnection {
        let future: EventLoopFuture<PostgresConnection> = makeConnection(logger: logger, on: eventLoop)
        return try await withCheckedThrowingContinuation { continuation in
            future.whenSuccess { val in
                continuation.resume(returning: val)
            }
            future.whenFailure { error in
                continuation.resume(throwing: error)
            }
        }
    }
}

extension PostgresConnection: ConnectionPoolItem {}
