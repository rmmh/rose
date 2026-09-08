package meta

import (
	"context"
	"database/sql"
	"fmt"
)

// Catalog triggers cover direct SQL mutations as well as Go helpers. Advancing
// a generation is atomic with the change that invalidates a captured placement.
// Identity, availability, geometry and durable-prefix changes all invalidate it.
func installPlacementEpochTriggers(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TRIGGER IF NOT EXISTS epoch_mapping_insert AFTER INSERT ON vlog_plog BEGIN
 UPDATE plog SET placement_epoch=placement_epoch+1 WHERE id=NEW.plog_id;
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id=NEW.vlog_id;
END;
CREATE TRIGGER IF NOT EXISTS epoch_mapping_delete AFTER DELETE ON vlog_plog BEGIN
 UPDATE plog SET placement_epoch=placement_epoch+1 WHERE id=OLD.plog_id;
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id=OLD.vlog_id;
END;
CREATE TRIGGER IF NOT EXISTS epoch_mapping_update AFTER UPDATE ON vlog_plog BEGIN
 UPDATE plog SET placement_epoch=placement_epoch+1 WHERE id IN (OLD.plog_id,NEW.plog_id);
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id IN (OLD.vlog_id,NEW.vlog_id);
END;
CREATE TRIGGER IF NOT EXISTS epoch_lease_insert AFTER INSERT ON vlog_lease BEGIN
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id=NEW.vlog_id;
END;
CREATE TRIGGER IF NOT EXISTS epoch_lease_delete AFTER DELETE ON vlog_lease BEGIN
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id=OLD.vlog_id;
END;
CREATE TRIGGER IF NOT EXISTS epoch_lease_update AFTER UPDATE ON vlog_lease BEGIN
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id IN (OLD.vlog_id,NEW.vlog_id);
END;
CREATE TRIGGER IF NOT EXISTS epoch_plog_insert AFTER INSERT ON plog BEGIN
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id IN (SELECT vlog_id FROM vlog_plog WHERE plog_id=NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS epoch_plog_update AFTER UPDATE OF disk_id,uid,length ON plog BEGIN
 UPDATE plog SET placement_epoch=placement_epoch+1 WHERE id=NEW.id;
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id IN (SELECT vlog_id FROM vlog_plog WHERE plog_id=NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS epoch_plog_delete BEFORE DELETE ON plog BEGIN
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id IN (SELECT vlog_id FROM vlog_plog WHERE plog_id=OLD.id);
END;
CREATE TRIGGER IF NOT EXISTS epoch_disk_update AFTER UPDATE OF state,node_id,uid ON disk BEGIN
 UPDATE plog SET placement_epoch=placement_epoch+1 WHERE disk_id=NEW.id;
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id IN (SELECT vp.vlog_id FROM vlog_plog vp JOIN plog p ON p.id=vp.plog_id WHERE p.disk_id=NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS epoch_disk_insert AFTER INSERT ON disk BEGIN
 UPDATE plog SET placement_epoch=placement_epoch+1 WHERE disk_id=NEW.id;
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id IN (SELECT vp.vlog_id FROM vlog_plog vp JOIN plog p ON p.id=vp.plog_id WHERE p.disk_id=NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS epoch_disk_delete BEFORE DELETE ON disk BEGIN
 UPDATE plog SET placement_epoch=placement_epoch+1 WHERE disk_id=OLD.id;
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id IN (SELECT vp.vlog_id FROM vlog_plog vp JOIN plog p ON p.id=vp.plog_id WHERE p.disk_id=OLD.id);
END;
CREATE TRIGGER IF NOT EXISTS epoch_node_update AFTER UPDATE OF state ON node BEGIN
 UPDATE plog SET placement_epoch=placement_epoch+1 WHERE disk_id IN (SELECT id FROM disk WHERE node_id=NEW.id);
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id IN (SELECT vp.vlog_id FROM vlog_plog vp JOIN plog p ON p.id=vp.plog_id JOIN disk d ON d.id=p.disk_id WHERE d.node_id=NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS epoch_node_insert AFTER INSERT ON node BEGIN
 UPDATE plog SET placement_epoch=placement_epoch+1 WHERE disk_id IN (SELECT id FROM disk WHERE node_id=NEW.id);
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id IN (SELECT vp.vlog_id FROM vlog_plog vp JOIN plog p ON p.id=vp.plog_id JOIN disk d ON d.id=p.disk_id WHERE d.node_id=NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS epoch_node_delete BEFORE DELETE ON node BEGIN
 UPDATE plog SET placement_epoch=placement_epoch+1 WHERE disk_id IN (SELECT id FROM disk WHERE node_id=OLD.id);
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id IN (SELECT vp.vlog_id FROM vlog_plog vp JOIN plog p ON p.id=vp.plog_id JOIN disk d ON d.id=p.disk_id WHERE d.node_id=OLD.id);
END;
CREATE TRIGGER IF NOT EXISTS epoch_vlog_update AFTER UPDATE OF uid,length,protection_scheme,data_shards,parity_shards,target_data_shards,target_parity_shards,dedup_domain,required_shards,maintenance_owned ON vlog BEGIN
 UPDATE vlog SET placement_epoch=placement_epoch+1 WHERE id=NEW.id;
END;
`)
	if err != nil {
		return fmt.Errorf("install placement generation triggers: %w", err)
	}
	return nil
}

// RepairDestinationEpoch captures the generation of a usable destination before
// physical I/O. The caller must retain topology/file ownership until repointing.
// An unassigned plog remains fenced even if its disk disappears and returns.
func (d *DB) RepairDestinationEpoch(ctx context.Context, plogID uint32) (int64, error) {
	var epoch int64
	err := d.db.QueryRowContext(ctx, `SELECT p.placement_epoch FROM plog p
 JOIN disk d ON d.id=p.disk_id JOIN node n ON n.id=d.node_id
 WHERE p.id=? AND d.state='active' AND n.state='working'
 AND NOT EXISTS (SELECT 1 FROM vlog_plog WHERE plog_id=p.id)`, plogID).Scan(&epoch)
	if err != nil {
		return 0, fmt.Errorf("capture repair destination %d: %w", plogID, err)
	}
	return epoch, nil
}
