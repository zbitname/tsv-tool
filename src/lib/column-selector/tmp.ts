import { transform } from 'stream-transform';
import fs from 'fs';

const COLUMN_DELIMITER = Buffer.from('\t');
const ROW_DELIMITER = Buffer.from('\n');
const COLUMN_DELIMITER_UINT8 = COLUMN_DELIMITER.readUInt8(0);
const ROW_DELIMITER_UINT8 = ROW_DELIMITER.readUInt8(0);
// const CHUNK_SIZE = 1024 * 1024 * 2;
const CHUNK_SIZE = 2 ** 21;

const selectedColumns = [0, 2].sort();
const maxColIdx = Math.max(...selectedColumns);
let prevBuffer = Buffer.alloc(0);

const transformTSV = (data: Buffer) => {
  const buf = Buffer.concat([prevBuffer, data]);
  const cache = Buffer.alloc(buf.byteLength * 2);

  let startRowPosition = 0;
  let finishRowPosition = buf.indexOf(ROW_DELIMITER_UINT8, startRowPosition);
  let colNumber = 0;
  let cacheOffset = 0;

  while (finishRowPosition >= 0) {
    const row = buf.subarray(startRowPosition, finishRowPosition);
    let startColPosition = 0;
    let finishColPosition = 0;

    colNumber = 0;
    finishColPosition = row.indexOf(COLUMN_DELIMITER_UINT8, startColPosition);

    while ((colNumber <= maxColIdx) || (finishColPosition < finishRowPosition && finishColPosition >= 0)) {
      if (selectedColumns.indexOf(colNumber) >= 0) {
        const v = row.subarray(startColPosition, finishColPosition);
        const l = v.byteLength;

        cache.fill(v, cacheOffset, l + cacheOffset);
        cacheOffset += l;

        if (colNumber === maxColIdx) {
          cache.fill(ROW_DELIMITER_UINT8, cacheOffset, cacheOffset + 1);
          cacheOffset++;
        } else {
          cache.fill(COLUMN_DELIMITER_UINT8, cacheOffset, cacheOffset + 1);
          cacheOffset++;
        }
      }

      startColPosition = finishColPosition + 1;
      colNumber++;
      finishColPosition = row.indexOf(COLUMN_DELIMITER_UINT8, startColPosition);

      if (finishColPosition === -1) {
        finishColPosition = finishRowPosition;
      }
    }

    startRowPosition = finishRowPosition + 1;
    finishRowPosition = buf.indexOf(ROW_DELIMITER_UINT8, startRowPosition);
  }

  prevBuffer = buf.subarray(startRowPosition);

  return cache.subarray(0, cacheOffset);
}

fs.createReadStream('./1.tsv', {
  highWaterMark: CHUNK_SIZE,
})
  .pipe(transform(transformTSV))
  .pipe(fs.createWriteStream('./5.res.tsv'));
