package net.corda.client.jfx

import net.corda.client.jfx.model.NodeMonitorModel
import net.corda.client.jfx.model.ProgressTrackingEvent
import net.corda.core.internal.bufferUntilSubscribed
import net.corda.core.contracts.Amount
import net.corda.core.contracts.ContractState
import net.corda.core.contracts.DOLLARS
import net.corda.core.contracts.USD
import net.corda.core.crypto.isFulfilledBy
import net.corda.core.crypto.keys
import net.corda.core.flows.FlowInitiator
import net.corda.core.flows.StateMachineRunId
import net.corda.core.identity.Party
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.StateMachineTransactionMapping
import net.corda.core.messaging.StateMachineUpdate
import net.corda.core.messaging.startFlow
import net.corda.core.node.services.NetworkMapCache
import net.corda.core.node.services.ServiceInfo
import net.corda.core.node.services.Vault
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.getOrThrow
import net.corda.flows.CashExitFlow
import net.corda.flows.CashIssueFlow
import net.corda.flows.CashPaymentFlow
import net.corda.node.services.network.NetworkMapService
import net.corda.node.services.startFlowPermission
import net.corda.node.services.transactions.SimpleNotaryService
import net.corda.nodeapi.User
import net.corda.testing.*
import net.corda.testing.driver.driver
import net.corda.testing.node.DriverBasedTest
import org.bouncycastle.asn1.x500.X500Name
import org.junit.Test
import rx.Observable

class NodeMonitorModelTest : DriverBasedTest() {
    lateinit var aliceNodeIdentity: Party
    lateinit var bobNodeIdentity: Party
    lateinit var notaryNodeIdentity: Party

    lateinit var rpc: CordaRPCOps
    lateinit var rpcBob: CordaRPCOps
    lateinit var stateMachineTransactionMapping: Observable<StateMachineTransactionMapping>
    lateinit var stateMachineUpdates: Observable<StateMachineUpdate>
    lateinit var stateMachineUpdatesBob: Observable<StateMachineUpdate>
    lateinit var progressTracking: Observable<ProgressTrackingEvent>
    lateinit var transactions: Observable<SignedTransaction>
    lateinit var vaultUpdates: Observable<Vault.Update<ContractState>>
    lateinit var networkMapUpdates: Observable<NetworkMapCache.MapChange>
    lateinit var newNodeIdentity: (X500Name) -> Party

    override fun setup() = driver {
        val cashUser = User("user1", "test", permissions = setOf(
                startFlowPermission<CashIssueFlow>(),
                startFlowPermission<CashPaymentFlow>(),
                startFlowPermission<CashExitFlow>())
        )
        val aliceNodeFuture = startNode(ALICE.name, rpcUsers = listOf(cashUser))
        val notaryNodeFuture = startNode(DUMMY_NOTARY.name, advertisedServices = setOf(ServiceInfo(SimpleNotaryService.type)))
        val aliceNodeHandle = aliceNodeFuture.getOrThrow()
        val notaryNodeHandle = notaryNodeFuture.getOrThrow()
        aliceNodeIdentity = aliceNodeHandle.mainIdentity
        notaryNodeIdentity = notaryNodeHandle.nodeInfo.notaryIdentity
        newNodeIdentity = { nodeName -> startNode(nodeName).getOrThrow().mainIdentity }
        val monitor = NodeMonitorModel()
        stateMachineTransactionMapping = monitor.stateMachineTransactionMapping.bufferUntilSubscribed()
        stateMachineUpdates = monitor.stateMachineUpdates.bufferUntilSubscribed()
        progressTracking = monitor.progressTracking.bufferUntilSubscribed()
        transactions = monitor.transactions.bufferUntilSubscribed()
        vaultUpdates = monitor.vaultUpdates.bufferUntilSubscribed()
        networkMapUpdates = monitor.networkMap.bufferUntilSubscribed()

        monitor.register(aliceNodeHandle.configuration.rpcAddress!!, cashUser.username, cashUser.password, initialiseSerialization = false)
        rpc = monitor.proxyObservable.value!!

        val bobNodeHandle = startNode(BOB.name, rpcUsers = listOf(cashUser)).getOrThrow()
        bobNodeIdentity = bobNodeHandle.mainIdentity
        val monitorBob = NodeMonitorModel()
        stateMachineUpdatesBob = monitorBob.stateMachineUpdates.bufferUntilSubscribed()
        monitorBob.register(bobNodeHandle.configuration.rpcAddress!!, cashUser.username, cashUser.password, initialiseSerialization = false)
        rpcBob = monitorBob.proxyObservable.value!!
        runTest()
    }

    @Test
    fun `network map update`() {
        newNodeIdentity(CHARLIE.name)
        networkMapUpdates.filter { !it.node.advertisedServices.any { it.info.type.isNotary() } }
                .filter { !it.node.advertisedServices.any { it.info.type == NetworkMapService.type } }
                .expectEvents(isStrict = false) {
                    sequence(
                            // TODO : Add test for remove when driver DSL support individual node shutdown.
                            expect { output: NetworkMapCache.MapChange ->
                                require(ALICE.name in output.node.legalIdentitiesAndCerts.map { it.name }) {
                                    "Expecting in identities set: ${ALICE.name}, Actual : ${output.node.legalIdentitiesAndCerts.map { it.name }}"
                                }
                            },
                            expect { output: NetworkMapCache.MapChange ->
                                require(BOB.name in output.node.legalIdentitiesAndCerts.map { it.name }) {
                                    "Expecting in identities set: ${BOB.name}, Actual : ${output.node.legalIdentitiesAndCerts.map { it.name }}"
                                }
                            },
                            expect { output: NetworkMapCache.MapChange ->
                                require(CHARLIE.name in output.node.legalIdentitiesAndCerts.map { it.name }) {
                                    "Expecting in identities set: ${CHARLIE.name}, Actual : ${output.node.legalIdentitiesAndCerts.map { it.name }}"
                                }

                            }
                    )
                }
    }

    @Test
    fun `cash issue works end to end`() {
        val anonymous = false
        rpc.startFlow(::CashIssueFlow,
                Amount(100, USD),
                OpaqueBytes(ByteArray(1, { 1 })),
                aliceNodeIdentity,
                notaryNodeIdentity,
                anonymous
        )

        vaultUpdates.expectEvents(isStrict = false) {
            sequence(
                    // SNAPSHOT
                    expect { (consumed, produced) ->
                        require(consumed.isEmpty()) { consumed.size }
                        require(produced.isEmpty()) { produced.size }
                    },
                    // ISSUE
                    expect { (consumed, produced) ->
                        require(consumed.isEmpty()) { consumed.size }
                        require(produced.size == 1) { produced.size }
                    }
            )
        }
    }

    @Test
    fun `cash issue and move`() {
        val anonymous = false
        rpc.startFlow(::CashIssueFlow, 100.DOLLARS, OpaqueBytes.of(1), aliceNodeIdentity, notaryNodeIdentity, anonymous).returnValue.getOrThrow()
        rpc.startFlow(::CashPaymentFlow, 100.DOLLARS, bobNodeIdentity, anonymous).returnValue.getOrThrow()

        var issueSmId: StateMachineRunId? = null
        var moveSmId: StateMachineRunId? = null
        var issueTx: SignedTransaction? = null
        var moveTx: SignedTransaction? = null
        stateMachineUpdates.expectEvents(isStrict = false) {
            sequence(
                    // ISSUE
                    expect { add: StateMachineUpdate.Added ->
                        issueSmId = add.id
                        val initiator = add.stateMachineInfo.initiator
                        require(initiator is FlowInitiator.RPC && initiator.username == "user1")
                    },
                    expect { remove: StateMachineUpdate.Removed ->
                        require(remove.id == issueSmId)
                    },
                    // MOVE - N.B. There are other framework flows that happen in parallel for the remote resolve transactions flow
                    expect(match = { it is StateMachineUpdate.Added && it.stateMachineInfo.flowLogicClassName == CashPaymentFlow::class.java.name }) { add: StateMachineUpdate.Added ->
                        moveSmId = add.id
                        val initiator = add.stateMachineInfo.initiator
                        require(initiator is FlowInitiator.RPC && initiator.username == "user1")
                    },
                    expect(match = { it is StateMachineUpdate.Removed && it.id == moveSmId }) {
                    }
            )
        }

        stateMachineUpdatesBob.expectEvents {
            sequence(
                    // MOVE
                    expect { add: StateMachineUpdate.Added ->
                        val initiator = add.stateMachineInfo.initiator
                        require(initiator is FlowInitiator.Peer && initiator.party.name == aliceNodeIdentity.name)
                    }
            )
        }

        transactions.expectEvents {
            sequence(
                    // ISSUE
                    expect { stx ->
                        require(stx.tx.inputs.isEmpty())
                        require(stx.tx.outputs.size == 1)
                        val signaturePubKeys = stx.sigs.map { it.by }.toSet()
                        // Only Alice signed
                        val aliceKey = aliceNodeIdentity.owningKey
                        require(signaturePubKeys.size <= aliceKey.keys.size)
                        require(aliceKey.isFulfilledBy(signaturePubKeys))
                        issueTx = stx
                    },
                    // MOVE
                    expect { stx ->
                        require(stx.tx.inputs.size == 1)
                        require(stx.tx.outputs.size == 1)
                        val signaturePubKeys = stx.sigs.map { it.by }.toSet()
                        // Alice and Notary signed
                        require(aliceNodeIdentity.owningKey.isFulfilledBy(signaturePubKeys))
                        require(notaryNodeIdentity.owningKey.isFulfilledBy(signaturePubKeys))
                        moveTx = stx
                    }
            )
        }

        vaultUpdates.expectEvents {
            sequence(
                    // SNAPSHOT
                    expect { (consumed, produced) ->
                        require(consumed.isEmpty()) { consumed.size }
                        require(produced.isEmpty()) { produced.size }
                    },
                    // ISSUE
                    expect { (consumed, produced) ->
                        require(consumed.isEmpty()) { consumed.size }
                        require(produced.size == 1) { produced.size }
                    },
                    // MOVE
                    expect { (consumed, produced) ->
                        require(consumed.size == 1) { consumed.size }
                        require(produced.isEmpty()) { produced.size }
                    }
            )
        }

        stateMachineTransactionMapping.expectEvents {
            sequence(
                    // ISSUE
                    expect { (stateMachineRunId, transactionId) ->
                        require(stateMachineRunId == issueSmId)
                        require(transactionId == issueTx!!.id)
                    },
                    // MOVE
                    expect { (stateMachineRunId, transactionId) ->
                        require(stateMachineRunId == moveSmId)
                        require(transactionId == moveTx!!.id)
                    }
            )
        }
    }
}
