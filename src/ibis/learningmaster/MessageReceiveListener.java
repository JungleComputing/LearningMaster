package ibis.learningmaster;

/**
 * The interface of a listener to a message receive port.
 * 
 * @author Kees van Reeuwijk
 */
interface MessageReceiveListener {
    /**
     * Handle the reception of message <code>packet</code>.
     * 
     * @param message
     *            The message that was received.
     */
    void messageReceived(Message message);
}
