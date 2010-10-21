package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

interface EngineInterface {
    /**
     * Tells the engine to start downloading the given piece from the given
     * peer.
     * 
     * @param peer
     *            The peer to download from.
     * @param piece
     *            The piece to download.
     */
    void startPieceDownload(IbisIdentifier peer, int piece);

    /**
     * Tells the engine to stop downloading the given piece from the given peer.
     * 
     * @param peer
     *            The peer to stop downloading from.
     * @param piece
     *            The piece to cancel downloading.
     */
    void cancelPieceDownload(IbisIdentifier peer, int piece);

    void wakeEngineThread();

    void setSuspect(IbisIdentifier destination);
}
