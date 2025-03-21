LINE_SIZE = 16  # 8 bytes price + 8 bytes amount
depth = int(input("Enter order book depth: "))
frequency = float(input("Enter collection frequency (in seconds): "))

if frequency <= 0:
    raise ValueError("Frequency must be greater than 0")

SNAPSHOT_SIZE = LINE_SIZE * depth * 2  # bids + asks

assets = int(input("Enter amount of assets to track: "))
months = float(input("Enter number of months to run (decimal number is valid): "))
compression_rate = float(input("Enter compression rate (> 0): "))
if compression_rate <= 0:
    raise ValueError("Compression rate must be greater than 0")

seconds = months * 30.44 * 24 * 60 * 60  # More accurate month duration
total_snapshots = seconds / frequency

storage_required = assets * SNAPSHOT_SIZE * total_snapshots
storage_required_mb = storage_required / (1024 ** 2)
storage_required_gb = storage_required_mb / 1024

print(f"\n📦 Uncompressed Storage for {months} months: {storage_required_mb:.2f} MB ({storage_required_gb:.2f} GB)")
print(f"📉 Compressed (×{compression_rate:.2f}): {storage_required_mb / compression_rate:.2f} MB ({storage_required_gb / compression_rate:.2f} GB)")
print(f"🧮 Total Snapshots per Asset: {int(total_snapshots)}")
print(f"🧮 Total Snapshots (All Assets): {int(total_snapshots * assets)}")
