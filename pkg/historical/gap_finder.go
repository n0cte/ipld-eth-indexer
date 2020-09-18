// VulcanizeDB
// Copyright © 2019 Vulcanize

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package historical

import "github.com/vulcanize/ipld-eth-indexer/pkg/postgres"

var (
	migrateEmptyGapsPgStr  = `INSERT INTO eth.gaps (start, stop)
				SELECT header_cids.block_number + 1 AS start, min(fr.block_number) - 1 AS stop FROM eth.header_cids
				LEFT JOIN eth.header_cids r on eth.header_cids.block_number = r.block_number - 1
				LEFT JOIN eth.header_cids fr on eth.header_cids.block_number < fr.block_number
				WHERE r.block_number is NULL and fr.block_number IS NOT NULL
				GROUP BY header_cids.block_number, r.block_number
				ON CONFLICT (start, stop) DO NOTHING`
	validationGapsPgStr = `SELECT block_number FROM eth.header_cids
				WHERE times_validated < $1
				ORDER BY block_number`
)
type GapFinder struct {
	db *postgres.DB
}

func NewGapFinder(db *postgres.DB) *GapFinder {
	return &GapFinder{
		db: db,
	}
}

// FindGaps is a background process for finding gaps in the database and storing them in the eth.gaps table
func (gf *GapFinder) FindGaps() error {

}

/*
Would be useful to talk through this gap issue because I'm having a hard time wrapping my head around how this new
table is a boon.
We create a new table to cache block ranges we need to backfill/resync
This table is empty to start, and the only way to populate it with the gaps that are currently in the db
we need to run the same type of SQL query that is currently limiting us.
Once it is populated, there needs to be a background process to periodically check for
new gaps in the database. This is again essentially the same query before except now it is even more expensive because of an
additional join on then new table to prevent us from inserting duplicate/overlapping gaps.

Here's where we get data races.

Gap finder looks for new gaps, finds some, but by the time it is done looking

Backfill checks out a new range from the gaps table to work over
it marks those gaps as being processed
While being processed, the gap finder goes and looks for new gaps
the search query now needs to do a really expensive join on the eth.gaps table to ensure the gap isn't duplicate/overlapping with the ones
already recorded.
When backFill is done with that range, it removes it from the eth.gaps table entirely

Once the eth.gaps table is empty, we switch to a faster- direct- backfilling that operates entirely with x blocks behind the current head

Need to figure out which indexes to create before

 */