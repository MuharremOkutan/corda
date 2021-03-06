package net.corda.core.transactions

import co.paralleluniverse.strands.Strand
import net.corda.core.contracts.*
import net.corda.core.crypto.*
import net.corda.core.identity.Party
import net.corda.core.internal.FlowStateMachine
import net.corda.core.node.ServiceHub
import net.corda.core.node.services.KeyManagementService
import java.lang.UnsupportedOperationException
import java.security.KeyPair
import java.security.PublicKey
import java.time.Duration
import java.time.Instant
import java.util.*

/**
 * A TransactionBuilder is a transaction class that's mutable (unlike the others which are all immutable). It is
 * intended to be passed around contracts that may edit it by adding new states/commands. Then once the states
 * and commands are right, this class can be used as a holding bucket to gather signatures from multiple parties.
 *
 * The builder can be customised for specific transaction types, e.g. where additional processing is needed
 * before adding a state/command.
 *
 * @param notary Notary used for the transaction. If null, this indicates the transaction DOES NOT have a notary.
 * When this is set to a non-null value, an output state can be added by just passing in a [ContractState] – a
 * [TransactionState] with this notary specified will be generated automatically.
 */
open class TransactionBuilder(
        var notary: Party? = null,
        var lockId: UUID = (Strand.currentStrand() as? FlowStateMachine<*>)?.id?.uuid ?: UUID.randomUUID(),
        protected val inputs: MutableList<StateRef> = arrayListOf(),
        protected val attachments: MutableList<SecureHash> = arrayListOf(),
        protected val outputs: MutableList<TransactionState<ContractState>> = arrayListOf(),
        protected val commands: MutableList<Command<*>> = arrayListOf(),
        protected var window: TimeWindow? = null,
        protected var privacySalt: PrivacySalt = PrivacySalt()
    ) {
    constructor(notary: Party) : this (notary, (Strand.currentStrand() as? FlowStateMachine<*>)?.id?.uuid ?: UUID.randomUUID())

    /**
     * Creates a copy of the builder.
     */
    fun copy() = TransactionBuilder(
            notary = notary,
            inputs = ArrayList(inputs),
            attachments = ArrayList(attachments),
            outputs = ArrayList(outputs),
            commands = ArrayList(commands),
            window = window,
            privacySalt = privacySalt
    )

    // DOCSTART 1
    /** A more convenient way to add items to this transaction that calls the add* methods for you based on type */
    fun withItems(vararg items: Any): TransactionBuilder {
        for (t in items) {
            when (t) {
                is StateAndRef<*> -> addInputState(t)
                is SecureHash -> addAttachment(t)
                is TransactionState<*> -> addOutputState(t)
                is StateAndContract -> addOutputState(t.state, t.contract)
                is ContractState -> throw UnsupportedOperationException("Removed as of V1: please use a StateAndContract instead")
                is Command<*> -> addCommand(t)
                is CommandData -> throw IllegalArgumentException("You passed an instance of CommandData, but that lacks the pubkey. You need to wrap it in a Command object first.")
                is TimeWindow -> setTimeWindow(t)
                is PrivacySalt -> setPrivacySalt(t)
                else -> throw IllegalArgumentException("Wrong argument type: ${t.javaClass}")
            }
        }
        return this
    }
    // DOCEND 1

    fun toWireTransaction() = WireTransaction(ArrayList(inputs), ArrayList(attachments),
            ArrayList(outputs), ArrayList(commands), notary, window, privacySalt)

    @Throws(AttachmentResolutionException::class, TransactionResolutionException::class)
    fun toLedgerTransaction(services: ServiceHub) = toWireTransaction().toLedgerTransaction(services)

    @Throws(AttachmentResolutionException::class, TransactionResolutionException::class, TransactionVerificationException::class)
    fun verify(services: ServiceHub) {
        toLedgerTransaction(services).verify()
    }

    open fun addInputState(stateAndRef: StateAndRef<*>): TransactionBuilder {
        val notary = stateAndRef.state.notary
        require(notary == this.notary) { "Input state requires notary \"$notary\" which does not match the transaction notary \"${this.notary}\"." }
        inputs.add(stateAndRef.ref)
        return this
    }

    fun addAttachment(attachmentId: SecureHash): TransactionBuilder {
        attachments.add(attachmentId)
        return this
    }

    fun addOutputState(state: TransactionState<*>): TransactionBuilder {
        outputs.add(state)
        return this
    }

    @JvmOverloads
    fun addOutputState(state: ContractState, contract: ContractClassName, notary: Party, encumbrance: Int? = null): TransactionBuilder {
        return addOutputState(TransactionState(state, contract, notary, encumbrance))
    }

    /** A default notary must be specified during builder construction to use this method */
    fun addOutputState(state: ContractState, contract: ContractClassName): TransactionBuilder {
        checkNotNull(notary) { "Need to specify a notary for the state, or set a default one on TransactionBuilder initialisation" }
        addOutputState(state, contract, notary!!)
        return this
    }

    fun addCommand(arg: Command<*>): TransactionBuilder {
        commands.add(arg)
        return this
    }

    fun addCommand(data: CommandData, vararg keys: PublicKey) = addCommand(Command(data, listOf(*keys)))
    fun addCommand(data: CommandData, keys: List<PublicKey>) = addCommand(Command(data, keys))

    /**
     * Sets the [TimeWindow] for this transaction, replacing the existing [TimeWindow] if there is one. To be valid, the
     * transaction must then be signed by the notary service within this window of time. In this way, the notary acts as
     * the Timestamp Authority.
     */
    fun setTimeWindow(timeWindow: TimeWindow): TransactionBuilder {
        check(notary != null) { "Only notarised transactions can have a time-window" }
        window = timeWindow
        return this
    }

    /**
     * The [TimeWindow] for the transaction can also be defined as [time] +/- [timeTolerance]. The tolerance should be
     * chosen such that your code can finish building the transaction and sending it to the Timestamp Authority within
     * that window of time, taking into account factors such as network latency. Transactions being built by a group of
     * collaborating parties may therefore require a higher time tolerance than a transaction being built by a single
     * node.
     */
    fun setTimeWindow(time: Instant, timeTolerance: Duration) = setTimeWindow(TimeWindow.withTolerance(time, timeTolerance))

    fun setPrivacySalt(privacySalt: PrivacySalt): TransactionBuilder {
        this.privacySalt = privacySalt
        return this
    }

    // Accessors that yield immutable snapshots.
    fun inputStates(): List<StateRef> = ArrayList(inputs)
    fun attachments(): List<SecureHash> = ArrayList(attachments)
    fun outputStates(): List<TransactionState<*>> = ArrayList(outputs)
    fun commands(): List<Command<*>> = ArrayList(commands)

    /**
     * Sign the built transaction and return it. This is an internal function for use by the service hub, please use
     * [ServiceHub.signInitialTransaction] instead.
     */
    fun toSignedTransaction(keyManagementService: KeyManagementService, publicKey: PublicKey, signatureMetadata: SignatureMetadata): SignedTransaction {
        val wtx = toWireTransaction()
        val signableData = SignableData(wtx.id, signatureMetadata)
        val sig = keyManagementService.sign(signableData, publicKey)
        return SignedTransaction(wtx, listOf(sig))
    }
}