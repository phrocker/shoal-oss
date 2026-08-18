#ifndef SHOAL_CAPI_TEST_SEAM_H
#define SHOAL_CAPI_TEST_SEAM_H

#ifndef SHOAL_TYPES_H
typedef struct shoal_batch_writer shoal_batch_writer;
#endif

enum {
  SHOAL_TEST_WRITER_SUCCESS = 0,
  SHOAL_TEST_WRITER_STRUCTURED_FAILURE = 1,
  SHOAL_TEST_WRITER_STICKY_DEADLINE = 2,
  SHOAL_TEST_WRITER_CONNECTOR_CLOSED = 3
};

int shoal_test_batch_writer_create(int mode, shoal_batch_writer **out_writer);

#endif
