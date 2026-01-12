# Frontend Requirements for Vehicle Soft Delete

## Overview

The backend now supports soft-deleting vehicles. When a vehicle is soft-deleted:
- It won't appear in the `latest_vehicle_positions` view (hidden from UI)
- The sync will skip it (won't be re-created even if Samsara/CAT still returns it)
- The record persists in the database to act as a blocklist

## Database Schema Changes

The `vehicles` table now has two new columns:

| Column | Type | Default | Description |
|--------|------|---------|-------------|
| `is_deleted` | boolean | `false` | When `true`, vehicle is hidden and blocked from sync |
| `deleted_at` | timestamptz | `null` | Timestamp when the vehicle was deleted |

## API: Soft Delete a Vehicle

To delete a vehicle, update its `is_deleted` flag:

```javascript
const softDeleteVehicle = async (vehicleId, organizationId) => {
  const { data, error } = await supabase
    .from('vehicles')
    .update({
      is_deleted: true,
      deleted_at: new Date().toISOString()
    })
    .eq('id', vehicleId)
    .eq('organization_id', organizationId)
    .eq('is_deleted', false);  // Only delete if not already deleted

  return { data, error };
};
```

## API: Restore a Deleted Vehicle

To restore a previously deleted vehicle:

```javascript
const restoreVehicle = async (vehicleId, organizationId) => {
  const { data, error } = await supabase
    .from('vehicles')
    .update({
      is_deleted: false,
      deleted_at: null
    })
    .eq('id', vehicleId)
    .eq('organization_id', organizationId)
    .eq('is_deleted', true);  // Only restore if currently deleted

  return { data, error };
};
```

## API: List Deleted Vehicles (Admin)

To show deleted vehicles in an admin panel:

```javascript
const getDeletedVehicles = async (organizationId) => {
  const { data, error } = await supabase
    .from('vehicles')
    .select('id, name, external_id, source_system, type, deleted_at')
    .eq('organization_id', organizationId)
    .eq('is_deleted', true)
    .order('deleted_at', { ascending: false });

  return { data, error };
};
```

## UI Implementation

### 1. Add Delete Button to Vehicle Card/Row

```jsx
// Example React component
const VehicleActions = ({ vehicle, organizationId, onDeleted }) => {
  const [confirming, setConfirming] = useState(false);

  const handleDelete = async () => {
    const { error } = await softDeleteVehicle(vehicle.vehicle_id, organizationId);
    if (!error) {
      onDeleted(vehicle.vehicle_id);
    }
  };

  if (confirming) {
    return (
      <div>
        <span>Remove {vehicle.vehicle_name} from tracking?</span>
        <button onClick={handleDelete}>Confirm</button>
        <button onClick={() => setConfirming(false)}>Cancel</button>
      </div>
    );
  }

  return (
    <button onClick={() => setConfirming(true)}>
      Delete
    </button>
  );
};
```

### 2. Confirmation Dialog Text

Suggested copy:
> "Remove [Vehicle Name] from tracking? This vehicle will no longer appear in ForeSyt, even if it's still in Samsara or CAT."

### 3. After Deletion

- Refresh the vehicle list (or remove from local state)
- Show success toast: "Vehicle removed from tracking"
- The `latest_vehicle_positions` view automatically excludes deleted vehicles

## Notes

- **No data loss**: The vehicle record stays in the database, just hidden
- **Permanent block**: Even if Samsara/CAT keeps returning this vehicle, it won't be re-added
- **Reversible**: Use the restore function to bring back a deleted vehicle
- **Organization-scoped**: Always filter by `organization_id` for security
