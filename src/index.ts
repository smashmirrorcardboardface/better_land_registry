import * as csv from 'fast-csv';
import * as fs from 'fs';
import * as fsExtra from 'fs-extra';
import * as path from 'path';

const BATCH_SIZE = 100; // Number of rows to process per batch
const MAX_ROWS_TO_PROCESS = 50; // Set this to 0 to process the entire file
const INPUT_FILE = path.join(__dirname, '../data', 'CCOD.csv');
const OUTPUT_FILE = path.join(__dirname, '../data', 'CCOD_enhanced.json');
const PROGRESS_FILE = path.join(__dirname, 'progress.json');
interface Progress {
  lastProcessedRow: number;
}

interface Proprietor {
  name: string;
  category: string;
  companiesHouseNumber?: string; // Optional field
  address1?: string; // Optional field for Address (1)
  address2?: string; // Optional field for Address (2)
  address3?: string; // Optional field for Address (3)
}

function getProgress(): Progress {
  if (fs.existsSync(PROGRESS_FILE)) {
    const progress = fs.readFileSync(PROGRESS_FILE, 'utf-8');
    return JSON.parse(progress);
  }
  return { lastProcessedRow: 0 };
}

function saveProgress(progress: Progress): void {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress));
}

function processRow(row: any): any {
  const processedRow: any = {};
  const proprietors: Proprietor[] = [];

  const proprietorNames: { [key: string]: string } = {};
  const proprietorCategories: { [key: string]: string } = {};
  const companyRegistrations: { [key: string]: string } = {};
  const proprietorAddresses1: { [key: string]: string } = {};
  const proprietorAddresses2: { [key: string]: string } = {};
  const proprietorAddresses3: { [key: string]: string } = {};

  for (const key in row) {
    if (row[key] !== null && row[key] !== undefined && row[key] !== '') {
      const nameMatch = key.match(/^Proprietor Name \((\d+)\)$/i);
      const categoryMatch = key.match(/^Proprietorship Category \((\d+)\)$/i);
      const registrationMatch = key.match(/^Company Registration No. \((\d+)\)$/i);
      const address1Match = key.match(/^Proprietor \((\d+)\) Address \(1\)$/i);
      const address2Match = key.match(/^Proprietor \((\d+)\) Address \(2\)$/i);
      const address3Match = key.match(/^Proprietor \((\d+)\) Address \(3\)$/i);

      if (nameMatch) {
        const index = nameMatch[1];
        proprietorNames[index] = row[key];
      } else if (categoryMatch) {
        const index = categoryMatch[1];
        proprietorCategories[index] = row[key];
      } else if (registrationMatch) {
        const index = registrationMatch[1];
        companyRegistrations[index] = row[key];
      } else if (address1Match) {
        const index = address1Match[1];
        proprietorAddresses1[index] = row[key];
      } else if (address2Match) {
        const index = address2Match[1];
        proprietorAddresses2[index] = row[key];
      } else if (address3Match) {
        const index = address3Match[1];
        proprietorAddresses3[index] = row[key];
      } else {
        processedRow[key] = row[key];
      }
    }
  }

  // Combine names, categories, registration numbers, and addresses into the proprietors array
  for (const index in proprietorNames) {
    const proprietor: Proprietor = {
      name: proprietorNames[index],
      category: proprietorCategories[index],
      companiesHouseNumber: companyRegistrations[index], // Optional
      address1: proprietorAddresses1[index], // Optional
      address2: proprietorAddresses2[index], // Optional
      address3: proprietorAddresses3[index], // Optional
    };
    proprietors.push(proprietor);
  }

  // Add the proprietors array if any were found
  if (proprietors.length > 0) {
    processedRow.proprietors = proprietors;
  }

  return processedRow;
}

async function processCSV() {
  let currentBatch = 0;
  let rowsProcessed = 0;
  const progress = getProgress();
  const jsonArray: any[] = [];
  let stopProcessing = false;

  const inputStream = fs.createReadStream(INPUT_FILE);

  const csvStream = csv
    .parse({ headers: true })
    .on('data', (row) => {
      if (stopProcessing) return;

      if (rowsProcessed >= progress.lastProcessedRow) {
        if (MAX_ROWS_TO_PROCESS && rowsProcessed >= progress.lastProcessedRow + MAX_ROWS_TO_PROCESS) {
          stopProcessing = true;
          saveProgress({ lastProcessedRow: rowsProcessed });
          writeJsonFile(jsonArray);
          inputStream.destroy(); // Close the input stream
          return;
        }

        const processedRow = processRow(row);
        jsonArray.push(processedRow);

        rowsProcessed++;
        currentBatch++;

        if (currentBatch >= BATCH_SIZE) {
          saveProgress({ lastProcessedRow: rowsProcessed });
          currentBatch = 0;
        }
      } else {
        rowsProcessed++;
      }
    })
    .on('end', () => {
      if (!stopProcessing) {
        saveProgress({ lastProcessedRow: rowsProcessed });
        writeJsonFile(jsonArray);
        console.log('Processing complete.');
      }
    })
    .on('error', (err) => {
      console.error('Error processing CSV:', err);
      writeJsonFile(jsonArray); // Write whatever data was processed before the error
    });

  inputStream.pipe(csvStream);
}

function writeJsonFile(jsonArray: any[]) {
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(jsonArray, null, 2));
  console.log(`JSON data written to ${OUTPUT_FILE}`);
  fsExtra.removeSync(PROGRESS_FILE);
}

processCSV().catch((err) => console.error(err));
