package meta

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rmmh/rose/uid"
)

// One bounded-size persistent counter prevents disk ID deletion/reinsertion
// from resetting a generation, even when the replacement repeats the old UID.
func installDiskEpochTriggers(db *sql.DB) error {
	for _, event := range []string{"INSERT", "UPDATE OF state,node_id,uid"} {
		name := "disk_generation_insert"
		if event != "INSERT" {
			name = "disk_generation_update"
		}
		_, err := db.Exec(fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS %s AFTER %s ON disk BEGIN
 UPDATE placement_clock SET epoch=epoch+1 WHERE id=1;
 UPDATE disk SET placement_epoch=(SELECT epoch FROM placement_clock WHERE id=1) WHERE id=NEW.id;
 END`, name, event))
		if err != nil {
			return err
		}
	}
	for _, event := range []string{"INSERT", "UPDATE OF state", "DELETE"} {
		timing, alias, name := "AFTER", "NEW", "node_generation_insert"
		if event == "DELETE" {
			timing, alias, name = "BEFORE", "OLD", "node_generation_delete"
		}
		if event == "UPDATE OF state" {
			name = "node_generation_update"
		}
		_, err := db.Exec(fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS %s %s %s ON node BEGIN
 UPDATE placement_clock SET epoch=epoch+1 WHERE id=1;
 UPDATE disk SET placement_epoch=(SELECT epoch FROM placement_clock WHERE id=1) WHERE node_id=%s.id;
 END`, name, timing, event, alias))
		if err != nil {
			return err
		}
	}
	return nil
}

type DiskPlacement struct {
	Epoch int64
	UID   uid.UID
}

// CaptureDiskPlacement binds the physical header identity to the generation
// checked at completion. Availability is rechecked by the placement transaction.
func (d *DB) CaptureDiskPlacement(ctx context.Context, id uint32) (DiskPlacement, error) {
	var token DiskPlacement
	var raw []byte
	err := d.db.QueryRowContext(ctx, "SELECT placement_epoch,uid FROM disk WHERE id=?", id).Scan(&token.Epoch, &raw)
	if err != nil {
		return token, err
	}
	token.UID, err = uid.FromBytes(raw)
	return token, err
}
