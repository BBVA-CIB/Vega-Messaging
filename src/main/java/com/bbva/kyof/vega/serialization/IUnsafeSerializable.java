package com.bbva.kyof.vega.serialization;

/**
 * Interface to implement in order to allow being serialized and deserialized using {@link UnsafeBufferSerializer}
 */
public interface IUnsafeSerializable
{
    /**
     * Fill internal fields from the binary representation contained in the buffer
     *
     * @param buffer the buffer serializer
     */
    void fromBinary(final UnsafeBufferSerializer buffer);

    /**
     * Convert the object to binary using the provided buffer serializer
     *
     * @param buffer the buffer serializer
     */
    void toBinary(final UnsafeBufferSerializer buffer);

    /**
     * Return the size the object would have if serialized
     * @return the serialized size of the object
     */
    int serializedSize();
}
