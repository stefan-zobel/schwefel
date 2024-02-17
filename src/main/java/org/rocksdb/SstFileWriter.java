// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;

/**
 * SstFileWriter is used to create sst files that can be added to the
 * database later. All keys in files generated by SstFileWriter will have
 * sequence number = 0.
 */
public class SstFileWriter extends RocksObject {
  /**
   * SstFileWriter Constructor.
   *
   * @param envOptions {@link org.rocksdb.EnvOptions} instance.
   * @param options {@link org.rocksdb.Options} instance.
   */
  public SstFileWriter(final EnvOptions envOptions, final Options options) {
    super(newSstFileWriter(
        envOptions.nativeHandle_, options.nativeHandle_));
  }

  /**
   * Prepare SstFileWriter to write to a file.
   *
   * @param filePath the location of file
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void open(final String filePath) throws RocksDBException {
    open(nativeHandle_, filePath);
  }

  /**
   * Add a Put key with value to currently opened file.
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final Slice key, final Slice value) throws RocksDBException {
    put(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  /**
   * Add a Put key with value to currently opened file.
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final DirectSlice key, final DirectSlice value)
      throws RocksDBException {
    put(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  /**
   * Add a Put key with value to currently opened file.
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final ByteBuffer key, final ByteBuffer value) throws RocksDBException {
    assert key.isDirect() && value.isDirect();
    putDirect(nativeHandle_, key, key.position(), key.remaining(), value, value.position(),
        value.remaining());
    key.position(key.limit());
    value.position(value.limit());
  }

 /**
   * Add a Put key with value to currently opened file.
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final byte[] key, final byte[] value) throws RocksDBException {
    put(nativeHandle_, key, value);
  }

  /**
   * Add a Merge key with value to currently opened file.
   *
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for
   * the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final Slice key, final Slice value)
      throws RocksDBException {
    merge(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  /**
   * Add a Merge key with value to currently opened file.
   *
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for
   * the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final byte[] key, final byte[] value)
      throws RocksDBException {
    merge(nativeHandle_, key, value);
  }

  /**
   * Add a Merge key with value to currently opened file.
   *
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for
   * the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final DirectSlice key, final DirectSlice value)
      throws RocksDBException {
    merge(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  /**
   * Add a deletion key to currently opened file.
   *
   * @param key the specified key to be deleted.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final Slice key) throws RocksDBException {
    delete(nativeHandle_, key.getNativeHandle());
  }

  /**
   * Add a deletion key to currently opened file.
   *
   * @param key the specified key to be deleted.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final DirectSlice key) throws RocksDBException {
    delete(nativeHandle_, key.getNativeHandle());
  }

  /**
   * Add a deletion key to currently opened file.
   *
   * @param key the specified key to be deleted.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final byte[] key) throws RocksDBException {
    delete(nativeHandle_, key);
  }

  /**
   * Finish the process and close the sst file.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void finish() throws RocksDBException {
    finish(nativeHandle_);
  }

  /**
   * Return the current file size.
   *
   * @return the current file size.
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public long fileSize() throws RocksDBException {
    return fileSize(nativeHandle_);
  }

  // (AP) Should we expose a constructor wrapping this ?
  private static native long newSstFileWriter(final long envOptionsHandle, final long optionsHandle,
      final long userComparatorHandle, final byte comparatorType);

  private static native long newSstFileWriter(
      final long envOptionsHandle, final long optionsHandle);

  private static native void open(final long handle, final String filePath) throws RocksDBException;

  private static native void put(final long handle, final long keyHandle, final long valueHandle)
      throws RocksDBException;

  private static native void put(final long handle, final byte[] key, final byte[] value)
      throws RocksDBException;

  private static native void putDirect(long handle, ByteBuffer key, int keyOffset, int keyLength,
      ByteBuffer value, int valueOffset, int valueLength) throws RocksDBException;

  private static native long fileSize(long handle) throws RocksDBException;

  private static native void merge(final long handle, final long keyHandle, final long valueHandle)
      throws RocksDBException;

  private static native void merge(final long handle, final byte[] key, final byte[] value)
      throws RocksDBException;

  private static native void delete(final long handle, final long keyHandle)
      throws RocksDBException;

  private static native void delete(final long handle, final byte[] key) throws RocksDBException;

  private static native void finish(final long handle) throws RocksDBException;

  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);
}
