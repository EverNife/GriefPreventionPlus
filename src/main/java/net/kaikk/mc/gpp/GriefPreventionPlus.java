/*
    GriefPreventionPlus Server Plugin for Minecraft
    Copyright (C) 2015 Antonino Kai Pocorobba
    (forked from GriefPrevention by Ryan Hamshire)

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.kaikk.mc.gpp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.logging.Logger;

import br.com.finalcraft.gpp.config.FCConfig;
import net.kaikk.mc.gpp.integration.EverNifeCoreIntegration;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.Chunk;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.OfflinePlayer;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.BlockFace;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.PlayerInventory;
import org.bukkit.plugin.PluginManager;
import org.bukkit.plugin.RegisteredServiceProvider;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.util.BlockIterator;

import net.milkbowl.vault.economy.Economy;
import net.milkbowl.vault.permission.Permission;

@SuppressWarnings("deprecation")
public class GriefPreventionPlus extends JavaPlugin {
	// for convenience, a reference to the instance of this plugin
	private static GriefPreventionPlus instance;

	// for logging to the console and log file
	private static Logger log = Logger.getLogger("Minecraft");

	/** UUID 0 is used for "public" permission and subclaims */
	public final static UUID UUID0 = new UUID(0, 0);

	/** UUID 1 is used for administrative claims */
	public final static UUID UUID1 = new UUID(0, 1);

	public static boolean isBukkit18 = false;

	// reference to the economy plugin, if economy integration is enabled
	public static Economy economy = null;

	// how far away to search from a tree trunk for its branch blocks
	public static final int TREE_RADIUS = 5;

	// how long to wait before deciding a player is staying online or staying
	// offline, for notication messages
	public static final int NOTIFICATION_SECONDS = 20;
	
	// this handles data storage, like player and region data
	private DataStore dataStore;

	// this tracks item stacks expected to drop which will need protection
	ArrayList<PendingItemProtection> pendingItemWatchList = new ArrayList<PendingItemProtection>();

	public Config config;

	// helper method to resolve a player by name
	public HashMap<String, UUID> playerNameToIDMap = new HashMap<String, UUID>();
	
	private Permission provider;
	
	@Override
	public void onLoad() {
		// check if Grief Prevention is loaded
		if (this.getServer().getPluginManager().getPlugin("GriefPrevention") != null) {
			addLogEntry("-- WARNING  --");
			addLogEntry("-- SHUTDOWN --");
			addLogEntry("Remove GriefPrevention.jar (do not delete data folder)");
			addLogEntry("--------------");
			this.getServer().shutdown();
			this.getServer().getPluginManager().clearPlugins();
			return;
		}
	}
	
	// initializes well... everything
	@Override
	public void onEnable() {

		addLogEntry("boot start.");
		setInstance(this);

		this.config = new Config();

		addLogEntry("Finished loading configuration.");

		// when datastore initializes, it loads player and claim data, and posts
		// some stats to the log
		if (this.config.databaseUrl.length() > 0 || this.config.useLocalYMLInstead) {
			try {
				final DataStore databaseStore;
				if (this.config.useLocalYMLInstead){
					try {
						FCConfig.class.toString();
					}catch (Throwable ignored){
						throw new IllegalStateException("To useLocalYMLInstead you need EverNifeCore 2.0.4 present because of YML performance storaged.");
					}
					databaseStore = new DataStoreYML();
				}else {
					databaseStore = new DataStoreMySQL(this.config.databaseUrl, this.config.databaseUserName, this.config.databasePassword);
				}
				databaseStore.initialize();
				this.setDataStore(databaseStore);
				addLogEntry("Using " + databaseStore.getClass().getSimpleName());
			} catch (final Exception e) {
				addLogEntry(e.getMessage());
				e.printStackTrace();
				addLogEntry("-- WARNING  --");
				addLogEntry("-- SHUTDOWN --");
				addLogEntry("I can't connect to the database! Update the database config settings to resolve the issue. The server will shutdown to avoid claim griefing.");
				addLogEntry("--------------");
				this.getServer().shutdown();
				this.getServer().getPluginManager().clearPlugins();
				return;
			}
		} else {
			addLogEntry("-- WARNING  --");
			addLogEntry("Database settings are required! Update the database config settings to resolve the issue. Grief Prevention Plus disabled.");
			addLogEntry("--------------");
			this.getPluginLoader().disablePlugin(this);
			return;
		}

		addLogEntry("Finished loading data.");
		
		
		// initialize Vault permission provider
		RegisteredServiceProvider<Permission> registeredService = Bukkit.getServicesManager().getRegistration(Permission.class);
		if (registeredService!=null) {
			provider = registeredService.getProvider();
		}

		// unless claim block accrual is disabled, start the recurring per 5
		// minute event to give claim blocks to online players
		// 20L ~ 1 second
		if (this.config.claims_blocksAccruedPerHour > 0) {
			final DeliverClaimBlocksTask task = new DeliverClaimBlocksTask(null);
			this.getServer().getScheduler().scheduleSyncRepeatingTask(this, task, 20L * 60 * 5, 20L * 60 * 5);
		}

		// register for events
		final PluginManager pluginManager = this.getServer().getPluginManager();

		// player events
		final PlayerEventHandler playerEventHandler = new PlayerEventHandler(this.getDataStore());
		pluginManager.registerEvents(playerEventHandler, this);

		// player events for MC 1.8
		try {
			Class.forName("org.bukkit.event.player.PlayerInteractAtEntityEvent");
			pluginManager.registerEvents(new EventHandler18(playerEventHandler), this);

			isBukkit18 = true;
			addLogEntry("Oh, you're running Bukkit 1.8+!");
		} catch (final ClassNotFoundException e) {
			addLogEntry("You're running Bukkit 1.7!");
		}

		// block events
		pluginManager.registerEvents(new BlockEventHandler(this.getDataStore()), this);

		// entity events
		pluginManager.registerEvents(new EntityEventHandler(this.getDataStore()), this);

		// if economy is enabled
		if ((this.config.economy_claimBlocksPurchaseCost > 0) || (this.config.economy_claimBlocksSellValue > 0)) {
			// try to load Vault
			GriefPreventionPlus.addLogEntry("GriefPrevention requires Vault for economy integration.");
			GriefPreventionPlus.addLogEntry("Attempting to load Vault...");
			final RegisteredServiceProvider<Economy> economyProvider = this.getServer().getServicesManager().getRegistration(net.milkbowl.vault.economy.Economy.class);
			GriefPreventionPlus.addLogEntry("Vault loaded successfully!");

			// ask Vault to hook into an economy plugin
			GriefPreventionPlus.addLogEntry("Looking for a Vault-compatible economy plugin...");
			if (economyProvider != null) {
				GriefPreventionPlus.economy = economyProvider.getProvider();

				// on success, display success message
				if (GriefPreventionPlus.economy != null) {
					GriefPreventionPlus.addLogEntry("Hooked into economy: " + GriefPreventionPlus.economy.getName() + ".");
					GriefPreventionPlus.addLogEntry("Ready to buy/sell claim blocks!");
				}

				// otherwise error message
				else {
					GriefPreventionPlus.addLogEntry("ERROR: Vault was unable to find a supported economy plugin.  Either install a Vault-compatible economy plugin, or set both of the economy config variables to zero.");
				}
			}

			// another error case
			else {
				GriefPreventionPlus.addLogEntry("ERROR: Vault was unable to find a supported economy plugin.  Either install a Vault-compatible economy plugin, or set both of the economy config variables to zero.");
			}
		}
		
		final CommandExec commandExec = new CommandExec();
		for (String command : this.getDescription().getCommands().keySet()) {
			this.getCommand(command).setExecutor(commandExec);
		}

		// start recurring cleanup scan for unused claims belonging to inactive players
		if (GriefPreventionPlus.getInstance().config.claims_expirationDays != 0 ||  GriefPreventionPlus.getInstance().config.claims_chestClaimExpirationDays != 0) {
			new CleanupUnusedClaimsTask(this).runTaskTimer(this, 200L, 1L);
		}
		
		addLogEntry("Boot finished.");
	}

	@Override
	public void onDisable() {
		if (this.getDataStore() != null) {
			// save data for any online players
			for (final Player player : this.getServer().getOnlinePlayers()) {
				final UUID playerID = player.getUniqueId();
				final PlayerData playerData = this.getDataStore().getPlayerData(playerID);
				this.getDataStore().savePlayerDataSync(playerID, playerData);
			}
			this.getDataStore().close();
		}

		addLogEntry("GriefPreventionPlus disabled.");
	}
	
	public String allowBreak(Player player, Block block, Location location) {
		final PlayerData playerData = this.getDataStore().getPlayerData(player.getUniqueId());
		final Claim claim = this.getDataStore().getClaimAt(location, false, playerData.lastClaim);

		// exception: administrators in ignore claims mode, and special player
		// accounts created by server mods
		if (playerData.ignoreClaims || this.config.mods_ignoreClaimsAccounts.contains(player.getName()) || (this.config.mods_ignoreAllFakePlayers && EverNifeCoreIntegration.isFakePlayer(player))  ) {
			return null;
		}

		// wilderness rules
		if (claim == null) {
			// no building in the wilderness in creative mode
			if (this.creativeRulesApply(location.getWorld()) || (this.config.claimRequiredWorlds.contains(location.getWorld().getName()))) {
				String reason = this.getDataStore().getMessage(Messages.NoBuildOutsideClaims);
				if (player.hasPermission("griefprevention.ignoreclaims")) {
					reason += "  " + this.getDataStore().getMessage(Messages.IgnoreClaimsAdvertisement);
				}
				reason += "  " + this.getDataStore().getMessage(Messages.CreativeBasicsVideo2, DataStore.CREATIVE_VIDEO_URL);
				return reason;
			}

			// but it's fine in survival mode
			else {
				return null;
			}
		} else {
			// if not in the wilderness, then apply claim rules (permissions, etc)
			return claim.canBreak(player, block.getType());
		}
	}

	public String allowBuild(Player player, Location location) {
		return this.allowBuild(player, location, location.getBlock().getType());
	}

	public String allowBuild(Player player, Location location, Material material) {
		final PlayerData playerData = this.getDataStore().getPlayerData(player.getUniqueId());
		final Claim claim = this.getDataStore().getClaimAt(location, false, playerData.lastClaim);

		// exception: administrators in ignore claims mode and special player
		// accounts created by server mods
		if (playerData.ignoreClaims || this.config.mods_ignoreClaimsAccounts.contains(player.getName()) || (this.config.mods_ignoreAllFakePlayers && EverNifeCoreIntegration.isFakePlayer(player))) {
			return null;
		}

		// wilderness rules
		if (claim == null) {
			// no building in the wilderness in creative mode
			if (this.creativeRulesApply(location.getWorld()) || (this.config.claimRequiredWorlds.contains(location.getWorld().getName()))) {
				String reason = this.getDataStore().getMessage(Messages.NoBuildOutsideClaims);
				if (player.hasPermission("griefprevention.ignoreclaims")) {
					reason += "  " + this.getDataStore().getMessage(Messages.IgnoreClaimsAdvertisement);
				}
				reason += "  " + this.getDataStore().getMessage(Messages.CreativeBasicsVideo2, DataStore.CREATIVE_VIDEO_URL);
				return reason;
			}

			// but it's fine in survival mode
			else {
				return null;
			}
		}

		// if not in the wilderness, then apply claim rules (permissions, etc)
		else {
			return claim.canBuild(player, material);
		}
	}

	// checks whether players can create claims in a world
	public boolean claimsEnabledForWorld(World world) {
		return this.claimsEnabledForWorld(world.getName());
	}
	
	public boolean claimsEnabledForWorld(String world) {
		return !this.config.disabledWorlds.contains(world);
	}

	// moves a player from the claim he's in to a nearby wilderness location
	public Location ejectPlayer(Player player) {
		// look for a suitable location
		Location candidateLocation = player.getLocation();

		for (;;) {
			final Claim claim = GriefPreventionPlus.getInstance().getDataStore().getClaimAt(candidateLocation, false);
			if (claim == null) {
				break;
			}
			candidateLocation = new Location(claim.getWorld(), claim.lesserX - 1, 0, claim.lesserZ - 1);
		}

		// find a safe height, a couple of blocks above the surface
		GuaranteeChunkLoaded(candidateLocation);
		
		int y=searchSuitableYLevel(candidateLocation.getWorld(), candidateLocation.getBlockX(), candidateLocation.getBlockZ());
		Location destination;
		if (y==-1) { // no suitable y level found... teleport to spawn.
			destination = this.getServer().getWorlds().get(0).getSpawnLocation();
		} else {
			destination = new Location(candidateLocation.getWorld(), candidateLocation.getBlockX(), y, candidateLocation.getBlockZ());
		}
		
		player.teleport(destination);
		return destination;
	}
	
	
	// This method search for a safe y level at the specified world, x, z location
	// It uses a custom algorithm that tries to skip empty blocks (fastforward)
	// Source: Kai's Random Teleport
	static int searchSuitableYLevel(World world, int x, int z) {
		int y=126; // Max y level
		int fastforward=4; // default fastforward
		if (world.getEnvironment()==World.Environment.THE_END) { // 
			fastforward=2; // fastforward for the end
		}
		
		Block block = world.getBlockAt(x, y, z);
		int c, solid=Integer.MAX_VALUE, shift=0;
		do {
			if (block.getType().isSolid()) {
				if (shift==fastforward && solid>y) { // Stop fastforward
					solid=y;
					y+=fastforward-1;
					block = block.getRelative(BlockFace.UP, fastforward-1);
					continue;
				} else {
					Block upperBlock = block.getRelative(BlockFace.UP);
					
					c=0;
					while(c<4) { // a y level is suitable if there are 4 empty blocks above a valid solid block
						if (upperBlock.getTypeId()!=0) { // if it's not empty
							if (upperBlock.isLiquid()) { // if you get a liquid, it could be an ocean or a lava lake... there won't be any good y level here.
								return -1;
							}
							break;
						}
						upperBlock = upperBlock.getRelative(BlockFace.UP);
						c++;
					}
					
					if (c==4) {
						return y; // Found a suitable y level
					} else {
						shift=4; // 
					}
				}
			} else {
				if (solid>y) { // fastforward mode
					shift=fastforward;
				} else {
					shift=1;
				}
			}
			
			y-=shift;
			block = block.getRelative(BlockFace.DOWN, shift);
		} while(y>0);
		
		return -1;
	}

	public DataStore getDataStore() {
		return this.dataStore;
	}

	public int getSeaLevel(World world) {
		final Integer overrideValue = this.config.seaLevelOverride.get(world.getName());
		if ((overrideValue == null) || (overrideValue == -1)) {
			return world.getSeaLevel();
		} else {
			return overrideValue;
		}
	}


	public OfflinePlayer resolvePlayer(String name) {
		// try online players first
		final Player targetPlayer = this.getServer().getPlayer(name);
		if (targetPlayer != null) {
			return targetPlayer;
		}

		final UUID bestMatchID = this.playerNameToIDMap.get(name.toLowerCase());

		if (bestMatchID == null) {
			OfflinePlayer offP = Bukkit.getOfflinePlayer(name);
			if (offP.hasPlayedBefore() || offP.isOnline()) {
				cacheUUIDNamePair(offP.getUniqueId(), offP.getName());
				return offP;
			}
			
			return null;
		}

		return this.getServer().getOfflinePlayer(bestMatchID);
	}

	public UUID resolvePlayerId(String name) {
		// try online players first
		UUID uuid = this.playerNameToIDMap.get(name.toLowerCase());
		if (uuid == null) {
			OfflinePlayer offP = Bukkit.getOfflinePlayer(name);
			if (offP.hasPlayedBefore() || offP.isOnline()) {
				cacheUUIDNamePair(offP.getUniqueId(), offP.getName());
				return offP.getUniqueId();
			}
		}
		return uuid;
	}

	public void restoreChunk(Chunk chunk, int miny, boolean aggressiveMode, long delayInTicks, Player playerReceivingVisualization) {
		// build a snapshot of this chunk, including 1 block boundary outside of
		// the chunk all the way around
		final int maxHeight = chunk.getWorld().getMaxHeight();
		final BlockSnapshot[][][] snapshots = new BlockSnapshot[18][maxHeight][18];
		final Block startBlock = chunk.getBlock(0, 0, 0);
		final Location startLocation = new Location(chunk.getWorld(), startBlock.getX() - 1, 0, startBlock.getZ() - 1);
		for (int x = 0; x < snapshots.length; x++) {
			for (int z = 0; z < snapshots[0][0].length; z++) {
				for (int y = 0; y < snapshots[0].length; y++) {
					final Block block = chunk.getWorld().getBlockAt(startLocation.getBlockX() + x, startLocation.getBlockY() + y, startLocation.getBlockZ() + z);
					snapshots[x][y][z] = new BlockSnapshot(block.getLocation(), block.getTypeId(), block.getData());
				}
			}
		}

		// create task to process those data in another thread
		final Location lesserBoundaryCorner = chunk.getBlock(0, 0, 0).getLocation();
		final Location greaterBoundaryCorner = chunk.getBlock(15, 0, 15).getLocation();

		// create task
		// when done processing, this task will create a main thread task to
		// actually update the world with processing results
		final RestoreNatureProcessingTask task = new RestoreNatureProcessingTask(snapshots, miny, chunk.getWorld().getEnvironment(), lesserBoundaryCorner.getBlock().getBiome(), lesserBoundaryCorner, greaterBoundaryCorner, this.getSeaLevel(chunk.getWorld()), aggressiveMode, GriefPreventionPlus.getInstance().creativeRulesApply(lesserBoundaryCorner.getWorld()), playerReceivingVisualization);
		GriefPreventionPlus.getInstance().getServer().getScheduler().runTaskLaterAsynchronously(GriefPreventionPlus.getInstance(), task, delayInTicks);
	}

	// restores nature in multiple chunks, as described by a claim instance
	// this restores all chunks which have ANY number of claim blocks from this
	// claim in them
	// if the claim is still active (in the data store), then the claimed blocks
	// will not be changed (only the area bordering the claim)
	public void restoreClaim(Claim claim, long delayInTicks) {
		// admin claims aren't automatically cleaned up when deleted or
		// abandoned
		if (claim.isAdminClaim()) {
			return;
		}

		// it's too expensive to do this for huge claims
		if (claim.getArea() > 10000) {
			return;
		}

		final ArrayList<Chunk> chunks = claim.getChunks();
		for (final Chunk chunk : chunks) {
			this.restoreChunk(chunk, this.getSeaLevel(chunk.getWorld()) - 15, false, delayInTicks, null);
		}
	}

	public void setDataStore(DataStore dataStore) {
		this.dataStore = dataStore;
	}
	// determines whether creative anti-grief rules apply at a location
	boolean creativeRulesApply(World world) {
		return this.creativeRulesApply(world.getName());
	}	

	public boolean creativeRulesApply(String world) {
		return this.config.creativeRulesWorlds.contains(world);
	}
	

	// adds a server log entry
	public static synchronized void addLogEntry(String entry) {
		log.info("[GriefPreventionPlus] " + entry);
	}

	public static String getfriendlyLocationString(Location location) {
		return (location.getWorld() != null ? location.getWorld().getName() : "unknown") + ": x" + location.getBlockX() + ", z" + location.getBlockZ();
	}

	public static GriefPreventionPlus getInstance() {
		return instance;
	}

	public static void setInstance(GriefPreventionPlus instance) {
		GriefPreventionPlus.instance = instance;
	}

	// ensures a piece of the managed world is loaded into server memory
	// (generates the chunk if necessary)
	private static void GuaranteeChunkLoaded(Location location) {
		final Chunk chunk = location.getChunk();
		while (!chunk.isLoaded() || !chunk.load(true));
	}

	// cache for player name lookups, to save searches of all offline players
	static void cacheUUIDNamePair(UUID playerID, String playerName) {
		// store the reverse mapping
		GriefPreventionPlus.getInstance().playerNameToIDMap.put(playerName.toLowerCase(), playerID);
	}

	static Block getTargetNonAirBlock(Player player, int maxDistance) throws IllegalStateException {
		final BlockIterator iterator = new BlockIterator(player.getLocation(), player.getEyeHeight(), maxDistance);
		Block result = player.getLocation().getBlock().getRelative(BlockFace.UP);
		while (iterator.hasNext()) {
			result = iterator.next();
			if (result.getType() != Material.AIR) {
				return result;
			}
		}

		return result;
	}

	static boolean isInventoryEmpty(Player player) {
		final PlayerInventory inventory = player.getInventory();
		final ItemStack[] armorStacks = inventory.getArmorContents();

		// check armor slots, stop if any items are found
		for (int i = 0; i < armorStacks.length; i++) {
			if (!((armorStacks[i] == null) || (armorStacks[i].getType() == Material.AIR))) {
				return false;
			}
		}

		// check other slots, stop if any items are found
		final ItemStack[] generalStacks = inventory.getContents();
		for (int i = 0; i < generalStacks.length; i++) {
			if (!((generalStacks[i] == null) || (generalStacks[i].getType() == Material.AIR))) {
				return false;
			}
		}

		return true;
	}

	// helper method to resolve a player name from the player's UUID
	static String lookupPlayerName(UUID playerID) {
		// parameter validation
		if (playerID == null) {
			return "somebody";
		}
		if (playerID == UUID0) {
			return "public";
		}
		if (playerID == UUID1) {
			return "an administrator";
		}

		// check the cache
		final OfflinePlayer player = GriefPreventionPlus.getInstance().getServer().getOfflinePlayer(playerID);
		if ((player.getName() != null) && !player.getName().isEmpty()) {
			return player.getName();
		} else {
			return "someone";
		}
	}

	// sends a color-coded message to a player
	static void sendMessage(Player player, ChatColor color, Messages messageID, long delayInTicks, String... args) {
		final String message = GriefPreventionPlus.getInstance().getDataStore().getMessage(messageID, args);
		sendMessage(player, color, message, delayInTicks);
	}

	// sends a color-coded message to a player
	static void sendMessage(Player player, ChatColor color, Messages messageID, String... args) {
		sendMessage(player, color, messageID, 0, args);
	}

	// sends a color-coded message to a player
	static void sendMessage(CommandSender player, ChatColor color, String message) {
		if ((message == null) || (message.length() == 0)) {
			return;
		}

		if (player == null) {
			GriefPreventionPlus.addLogEntry(color + message);
		} else {
			player.sendMessage(color + message);
		}
	}

	static void sendMessage(Player player, ChatColor color, String message, long delayInTicks) {
		final SendPlayerMessageTask task = new SendPlayerMessageTask(player, color, message);
		if (delayInTicks > 0) {
			GriefPreventionPlus.getInstance().getServer().getScheduler().runTaskLater(GriefPreventionPlus.getInstance(), task, delayInTicks);
		} else {
			task.run();
		}
	}
	
	public boolean hasPermission(OfflinePlayer player, String permission) {
		try {
			return provider.playerHas(null, player, permission);
		} catch (Exception e) {
			return player.isOnline() ? player.getPlayer().hasPermission(permission) : false;
		}
	}
}
