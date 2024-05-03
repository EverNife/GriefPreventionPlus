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

import net.kaikk.mc.gpp.events.ClaimEnterEvent;
import net.kaikk.mc.gpp.events.ClaimExitEvent;
import net.kaikk.mc.gpp.events.ClaimCreationCornerSetEvent;
import net.kaikk.mc.gpp.events.ClaimFromToEvent;
import net.kaikk.mc.gpp.integration.EverNifeCoreIntegration;
import net.kaikk.mc.gpp.visualization.Visualization;
import net.kaikk.mc.gpp.visualization.VisualizationType;
import org.bukkit.*;
import org.bukkit.World.Environment;
import org.bukkit.block.Block;
import org.bukkit.block.BlockFace;
import org.bukkit.entity.*;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.Action;
import org.bukkit.event.entity.PlayerDeathEvent;
import org.bukkit.event.player.*;
import org.bukkit.event.player.PlayerTeleportEvent.TeleportCause;
import org.bukkit.inventory.InventoryHolder;
import org.bukkit.inventory.ItemStack;
import org.bukkit.plugin.PluginManager;
import org.bukkit.util.BlockIterator;

import java.util.*;
import java.util.regex.Pattern;

@SuppressWarnings("deprecation")
class PlayerEventHandler implements Listener {
	private final DataStore dataStore;

	// regex pattern for the "how do i claim land?" scanner
	private Pattern howToClaimPattern;

	// determines whether a block type is an inventory holder. uses a caching
	// strategy to save cpu time
	private final HashMap<Integer, Boolean> inventoryHolderCache = new HashMap<Integer, Boolean>();

	// typical constructor, yawn
	PlayerEventHandler(DataStore dataStore) {
		this.dataStore = dataStore;
		this.howToClaimPattern = Pattern.compile(this.dataStore.getMessage(Messages.HowToClaimRegex), Pattern.CASE_INSENSITIVE);
	}

	// when a player switches in-hand items
	@EventHandler(ignoreCancelled = true, priority = EventPriority.MONITOR)
	void onItemHeldChange(PlayerItemHeldEvent event) {
		final Player player = event.getPlayer();

		// if he's switching to the golden shovel
		final ItemStack newItemStack = player.getInventory().getItem(event.getNewSlot());
		if ((newItemStack != null) && (newItemStack.getType() == GriefPreventionPlus.getInstance().config.claims_modificationTool)) {
			final PlayerData playerData = GriefPreventionPlus.getInstance().getDataStore().getPlayerData(player.getUniqueId());

			// always reset to basic claims mode
			if (playerData.shovelMode != ShovelMode.Basic) {
				playerData.shovelMode = ShovelMode.Basic;
				GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.ShovelBasicClaimMode);
			}

			// reset any work he might have been doing
			playerData.lastShovelLocation = null;
			playerData.claimResizing = null;

			// give the player his available claim blocks count and claiming
			// instructions, but only if he keeps the shovel equipped for a
			// minimum time, to avoid mouse wheel spam
			if (GriefPreventionPlus.getInstance().claimsEnabledForWorld(player.getWorld())) {
				final EquipShovelProcessingTask task = new EquipShovelProcessingTask(player);
				GriefPreventionPlus.getInstance().getServer().getScheduler().scheduleSyncDelayedTask(GriefPreventionPlus.getInstance(), task, 15L);
			}
		}
	}

	// block use of buckets within other players' claims
	@EventHandler(ignoreCancelled = true, priority = EventPriority.LOWEST)
	public void onPlayerBucketEmpty(PlayerBucketEmptyEvent bucketEvent) {
		if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(bucketEvent.getBlockClicked().getWorld())) {
			return;
		}

		final Player player = bucketEvent.getPlayer();
		final Block block = bucketEvent.getBlockClicked().getRelative(bucketEvent.getBlockFace());
		int minLavaDistance = 10;

		// make sure the player is allowed to build at the location
		final String noBuildReason = GriefPreventionPlus.getInstance().allowBuild(player, block.getLocation(), Material.WATER);
		if (noBuildReason != null) {
			GriefPreventionPlus.sendMessage(player, TextMode.Err, noBuildReason);
			bucketEvent.setCancelled(true);
			return;
		}

		// if the bucket is being used in a claim, allow for dumping lava closer
		// to other players
		final PlayerData playerData = this.dataStore.getPlayerData(player.getUniqueId());
		final Claim claim = this.dataStore.getClaimAt(block.getLocation(), false, playerData.lastClaim);
		if (claim != null) {
			minLavaDistance = 3;
		} else if (GriefPreventionPlus.getInstance().creativeRulesApply(block.getWorld())) {
			// otherwise no wilderness dumping in creative mode worlds
			if ((block.getY() >= (GriefPreventionPlus.getInstance().getSeaLevel(block.getWorld()) - 5)) && !player.hasPermission("griefprevention.lava")) {
				if (bucketEvent.getBucket() == Material.LAVA_BUCKET) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NoWildernessBuckets);
					bucketEvent.setCancelled(true);
					return;
				}
			}
		}

		// lava buckets can't be dumped near other players unless pvp is on
		if (!GriefPreventionPlus.getInstance().config.pvp_enabledWorlds.contains(block.getWorld().getUID()) && !player.hasPermission("griefprevention.lava")) {
			if (bucketEvent.getBucket() == Material.LAVA_BUCKET) {
				final List<Player> players = block.getWorld().getPlayers();
				for (int i = 0; i < players.size(); i++) {
					final Player otherPlayer = players.get(i);
					final Location location = otherPlayer.getLocation();
					if (!otherPlayer.equals(player) && (otherPlayer.getGameMode() == GameMode.SURVIVAL) && (block.getY() >= (location.getBlockY() - 1)) && (location.distanceSquared(block.getLocation()) < (minLavaDistance * minLavaDistance))) {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NoLavaNearOtherPlayer, "another player");
						bucketEvent.setCancelled(true);
						return;
					}
				}
			}
		}
	}

	// see above
	@EventHandler(ignoreCancelled = true, priority = EventPriority.LOWEST)
	void onPlayerBucketFill(PlayerBucketFillEvent bucketEvent) {
		final Player player = bucketEvent.getPlayer();
		final Block block = bucketEvent.getBlockClicked();

		if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(block.getWorld())) {
			return;
		}

		// make sure the player is allowed to build at the location
		final String noBuildReason = GriefPreventionPlus.getInstance().allowBuild(player, block.getLocation(), Material.AIR);
		if (noBuildReason != null) {
			// exemption for cow milking (permissions will be handled by player
			// interact with entity event instead)
			final Material blockType = block.getType();
			if ((blockType == Material.AIR) || blockType.isSolid()) {
				return;
			}

			GriefPreventionPlus.sendMessage(player, TextMode.Err, noBuildReason);
			bucketEvent.setCancelled(true);
			return;
		}
	}

	// when a player interacts with an entity...
	@EventHandler(ignoreCancelled = true, priority = EventPriority.LOWEST)
	void onPlayerInteractEntity(PlayerInteractEntityEvent event) {
		final Player player = event.getPlayer();
		final Entity entity = event.getRightClicked();

		if (GriefPreventionPlus.getInstance().config.mods_ignoreAllFakePlayers && EverNifeCoreIntegration.isFakePlayer(player)){
			return;
		}

		if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(entity.getWorld())) {
			return;
		}

		// allow horse protection to be overridden to allow management from
		// other plugins
		if (!GriefPreventionPlus.getInstance().config.claims_protectHorses && (entity instanceof Horse)) {
			return;
		}

		final PlayerData playerData = this.dataStore.getPlayerData(player.getUniqueId());

		// if entity is tameable and has an owner, apply special rules
		if (entity instanceof Tameable) {
			final Tameable tameable = (Tameable) entity;
			if (tameable.isTamed() && (tameable.getOwner() != null)) {
				final UUID ownerID = tameable.getOwner().getUniqueId();

				// if the player interacting is the owner or an admin in ignore
				// claims mode, always allow
				if (player.getUniqueId().equals(ownerID) || playerData.ignoreClaims) {
					// if giving away pet, do that instead
					if (playerData.petGiveawayRecipient != null) {
						tameable.setOwner(playerData.petGiveawayRecipient);
						playerData.petGiveawayRecipient = null;
						GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.PetGiveawayConfirmation);
						event.setCancelled(true);
					}

					return;
				}

				if (!GriefPreventionPlus.getInstance().config.pvp_enabledWorlds.contains(entity.getLocation().getWorld().getUID())) {
					// otherwise disallow
					final OfflinePlayer owner = GriefPreventionPlus.getInstance().getServer().getOfflinePlayer(ownerID);
					String ownerName = owner.getName();
					if (ownerName == null) {
						ownerName = "someone";
					}
					String message = GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.NotYourPet, ownerName);
					if (player.hasPermission("griefprevention.ignoreclaims")) {
						message += "  " + GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.IgnoreClaimsAdvertisement);
					}
					GriefPreventionPlus.sendMessage(player, TextMode.Err, message);
					event.setCancelled(true);
					return;
				}
			}
		}

		// don't allow interaction with item frames or armor stands in claimed
		// areas without build permission
		if ((GriefPreventionPlus.isBukkit18 && MC18Helper.isArmorStatue(entity)) || (entity instanceof Hanging)) {
			final String noBuildReason = GriefPreventionPlus.getInstance().allowBuild(player, entity.getLocation(), Material.ITEM_FRAME);
			if (noBuildReason != null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, noBuildReason);
				event.setCancelled(true);
				return;
			}
		}

		// always allow interactions when player is in ignore claims mode
		if (playerData.ignoreClaims) {
			return;
		}

		// if the entity is a vehicle and we're preventing theft in claims
		if (GriefPreventionPlus.getInstance().config.claims_preventTheft && (entity instanceof Vehicle)) {
			// if the entity is in a claim
			final Claim claim = this.dataStore.getClaimAt(entity.getLocation(), false);
			if (claim != null) {
				// for storage entities, apply container rules (this is a
				// potential theft)
				if (entity instanceof InventoryHolder) {
					final String noContainersReason = claim.canOpenContainers(player);
					if (noContainersReason != null) {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, noContainersReason);
						event.setCancelled(true);
						return;
					}
				}

				// for boats, apply access rules
				else if (entity instanceof Boat) {
					final String noAccessReason = claim.canAccess(player);
					if (noAccessReason != null) {
						player.sendMessage(noAccessReason);
						event.setCancelled(true);
						return;
					}
				}
			}
		}

		// if the entity is an animal, apply container rules
		if (GriefPreventionPlus.getInstance().config.claims_preventTheft && ((entity instanceof Animals) || (entity.getType() == EntityType.VILLAGER))) {
			// if the entity is in a claim
			final Claim claim = this.dataStore.getClaimAt(entity.getLocation(), false);
			if (claim != null) {
				if (claim.canOpenContainers(player) != null) {
					String message = GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.NoDamageClaimedEntity, claim.getOwnerName());
					if (player.hasPermission("griefprevention.ignoreclaims")) {
						message += "  " + GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.IgnoreClaimsAdvertisement);
					}
					GriefPreventionPlus.sendMessage(player, TextMode.Err, message);
					event.setCancelled(true);
					return;
				}
			}
		}

		// if preventing theft, prevent leashing claimed creatures
		if (GriefPreventionPlus.getInstance().config.claims_preventTheft && (entity instanceof Creature) && (player.getItemInHand().getType() == Material.LEASH)) {
			final Claim claim = this.dataStore.getClaimAt(entity.getLocation(), false, playerData.lastClaim);
			if (claim != null) {
				final String failureReason = claim.canOpenContainers(player);
				if (failureReason != null) {
					event.setCancelled(true);
					GriefPreventionPlus.sendMessage(player, TextMode.Err, failureReason);
					return;
				}
			}
		}
	}

	// this event is fired when a player enters another claim/subdivision
	@EventHandler(ignoreCancelled = false, priority = EventPriority.HIGH)
	void onPlayerMove(PlayerMoveEvent event) {

		if (GriefPreventionPlus.getInstance().config.mods_ignoreAllFakePlayers && EverNifeCoreIntegration.isFakePlayer(event.getPlayer())){
			return;
		}

		if (!this.updateLastMovementClaim(event.getPlayer(), event.getFrom(), event.getTo())) {
			final Location invertedLocation = this.invertedLocation(event.getFrom(), event.getTo());
			final Claim claim = this.dataStore.getClaimAt(invertedLocation, false);
			if (claim == null || claim.canAccess(event.getPlayer())==null) {
				Block b2=invertedLocation.getWorld().getBlockAt(invertedLocation);
				if (b2.isEmpty() && b2.getRelative(BlockFace.UP).isEmpty()) {
					event.getPlayer().teleport(invertedLocation);
					return;
				}
			}
			
			GriefPreventionPlus.getInstance().ejectPlayer(event.getPlayer());
		}
	}

	// when a player teleports
	@EventHandler(ignoreCancelled = true, priority = EventPriority.HIGH)
	void onPlayerTeleport(PlayerTeleportEvent event) {
		if (GriefPreventionPlus.getInstance().config.mods_ignoreAllFakePlayers && EverNifeCoreIntegration.isFakePlayer(event.getPlayer())){
			return;
		}
		event.setCancelled(!this.updateLastMovementClaim(event.getPlayer(), event.getFrom(), event.getTo()));
	}

	// returns true if the message should be sent, false if it should be muted
	private boolean handlePlayerChat(Player player, String message, PlayerEvent event) {
		// FEATURE: automatically educate players about claiming land
		// watching for message format how*claim*, and will send a link to the
		// basics video
		if (this.howToClaimPattern.matcher(message).matches()) {
			if (GriefPreventionPlus.getInstance().creativeRulesApply(player.getWorld())) {
				GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.CreativeBasicsVideo2, 10L, DataStore.CREATIVE_VIDEO_URL);
			} else {
				GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.SurvivalBasicsVideo2, 10L, DataStore.SURVIVAL_VIDEO_URL);
			}
		}

		// FEATURE: automatically educate players about the /trapped command
		// check for "trapped" or "stuck" to educate players about the /trapped
		// command
		if (!message.contains("/trapped") && (message.contains("trapped") || message.contains("stuck") || message.contains(this.dataStore.getMessage(Messages.TrappedChatKeyword)))) {
			GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.TrappedInstructions, 10L);
		}

		return false;
	}

	private Location invertedLocation(Location from, Location to) {
		double dx = (to.getX() - from.getX());
		double dz = (to.getZ() - from.getZ());
		if (dx>=0) {
			dx+=2;
		} else {
			dx-=2;
		}
		if (dz>=0) {
			dz+=2;
		} else {
			dz-=2;
		}
		
		final double x = from.getX() - dx;
		final double y = from.getY() - ((to.getY() - from.getY()));
		final double z = from.getZ() - dz;
		float yaw = from.getYaw() + 180;
		if (yaw>360) {
			yaw-=360;
		}

		return new Location(from.getWorld(), x, y, z, yaw, from.getPitch());
	}

	private boolean isInventoryHolder(Block clickedBlock) {
		final Integer cacheKey = clickedBlock.getTypeId();
		final Boolean cachedValue = this.inventoryHolderCache.get(cacheKey);
		if (cachedValue != null) {
			return cachedValue.booleanValue();

		} else {
			final boolean isHolder = clickedBlock.getState() instanceof InventoryHolder;
			this.inventoryHolderCache.put(cacheKey, isHolder);
			return isHolder;
		}
	}

	private boolean onLeftClickWatchList(Material material) {
		switch (material) {
		case WOOD_BUTTON:
		case STONE_BUTTON:
		case LEVER:
		case DIODE_BLOCK_ON: // redstone repeater
		case DIODE_BLOCK_OFF:
		case CAKE_BLOCK:
			return true;
		default:
			return false;
		}
	}

	private boolean updateLastMovementClaim(Player player,  Location from, Location to) {
		final PlayerData playerData = this.dataStore.getPlayerData(player.getUniqueId());

		if (playerData.lastX!=to.getBlockX() || playerData.lastZ!=to.getBlockZ() || !to.getWorld().getUID().equals(playerData.lastWorld)) {
			final Claim claim = this.dataStore.getClaimAt(to, false);
			if (claim != null) {
				final String noEntryReason = claim.canEnter(player);
				if (noEntryReason != null) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, noEntryReason);
					return false;
				}
			}

			if (claim != playerData.lastClaim) {
				final PluginManager pm = GriefPreventionPlus.getInstance().getServer().getPluginManager();
				if (playerData.lastClaim != null) {
					if (claim != null) {
						final ClaimFromToEvent event = new ClaimFromToEvent(player, playerData.lastClaim, claim, from, to);
						pm.callEvent(event);
						if (event.isCancelled()) {
							return false;
						}
					} else {
						final ClaimExitEvent event = new ClaimExitEvent(playerData.lastClaim, player, from, to);
						pm.callEvent(event);
						if (event.isCancelled()) {
							return false;
						}
					}
				} else if (claim != null) {
					final ClaimEnterEvent event = new ClaimEnterEvent(claim, player, from, to);
					pm.callEvent(event);
					if (event.isCancelled()) {
						return false;
					}
				}

				playerData.lastClaim = claim;
				playerData.lastX = to.getBlockX();
				playerData.lastZ = to.getBlockZ();
				playerData.lastWorld = to.getWorld().getUID();
			}
		}

		return true;
	}

	// when a player chats, monitor for spam
	@EventHandler(ignoreCancelled = true, priority = EventPriority.LOWEST)
	synchronized void onPlayerChat(AsyncPlayerChatEvent event) {
		final Player player = event.getPlayer();
		if (!player.isOnline()) {
			event.setCancelled(true);
			return;
		}

		this.handlePlayerChat(player, event.getMessage(), event);
	}

	// when a player uses a slash command...
	@EventHandler(ignoreCancelled = true, priority = EventPriority.LOWEST)
	synchronized void onPlayerCommandPreprocess(PlayerCommandPreprocessEvent event) {
		final String[] args = event.getMessage().split(" ");

		// if requires access trust, check for permission
		boolean isMonitoredCommand = false;
		for (final String monitoredCommand : GriefPreventionPlus.getInstance().config.claims_commandsRequiringAccessTrust) {
			if (args[0].equalsIgnoreCase(monitoredCommand)) {
				isMonitoredCommand = true;
				break;
			}
		}

		if (isMonitoredCommand) {
			final Player player = event.getPlayer();
			final PlayerData playerData = this.dataStore.getPlayerData(player.getUniqueId());
			final Claim claim = this.dataStore.getClaimAt(player.getLocation(), false, playerData.lastClaim);
			if (claim != null) {
				final String reason = claim.canAccess(player);
				if (reason != null) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, reason);
					event.setCancelled(true);
				}
			}
		}
	}

	// when a player dies...
	@EventHandler(priority = EventPriority.LOWEST)
	void onPlayerDeath(PlayerDeathEvent event) {
		final PlayerData playerData = this.dataStore.getPlayerData(event.getEntity().getUniqueId());

		// Log when a player has been killed in his own claim
		final Player player = event.getEntity();
		final Player killer = player.getKiller();
		if (killer != null) {
			final Claim claim = GriefPreventionPlus.getInstance().getDataStore().getClaimAt(player.getLocation(), true, playerData.lastClaim);
			if (claim != null) {
				GriefPreventionPlus.addLogEntry(player.getName() + " has been killed on claim id " + claim.id + " by " + killer.getName());
			}
		}
	}

	// when a player interacts with the world
	@EventHandler(priority = EventPriority.HIGH)
	void onPlayerInteract(PlayerInteractEvent event) {
		final Action action = event.getAction();

		final Player player = event.getPlayer();

		if (GriefPreventionPlus.getInstance().config.mods_ignoreAllFakePlayers && EverNifeCoreIntegration.isFakePlayer(player)){
			return;
		}

		Block clickedBlock = event.getClickedBlock(); // null returned here
		// means interacting
		// with air

		Material clickedBlockType = null;
		if (clickedBlock != null) {
			clickedBlockType = clickedBlock.getType();
		} else {
			clickedBlockType = Material.AIR;
		}

		PlayerData playerData = null;
		if (!event.isCancelled()) {
			// don't care about left-clicking on most blocks, this is probably a
			// break action

			if ((action == Action.LEFT_CLICK_BLOCK) && (clickedBlock != null)) {
				// exception for blocks on a specific watch list
				if (!this.onLeftClickWatchList(clickedBlockType) && !GriefPreventionPlus.getInstance().config.mods_accessTrustIds.contains(new MaterialInfo(clickedBlock.getType(), clickedBlock.getData(), null))) {
					// and an exception for putting our fires
					if (GriefPreventionPlus.getInstance().config.claims_protectFires && (event.getClickedBlock() != null)) {
						final Block adjacentBlock = event.getClickedBlock().getRelative(event.getBlockFace());
						if (adjacentBlock.getType() == Material.FIRE) {
							if (playerData == null) {
								playerData = this.dataStore.getPlayerData(player.getUniqueId());
							}
							final Claim claim = this.dataStore.getClaimAt(clickedBlock.getLocation(), false, playerData.lastClaim);
							if (claim != null) {
								final String noBuildReason = claim.canBuild(player, Material.AIR);
								if (noBuildReason != null) {
									event.setCancelled(true);
									GriefPreventionPlus.sendMessage(player, TextMode.Err, noBuildReason);
									player.sendBlockChange(adjacentBlock.getLocation(), adjacentBlock.getTypeId(), adjacentBlock.getData());
									return;
								}
							}
						}
					}

					return;
				}
			}

			// apply rules for containers and crafting blocks
			if ((clickedBlock != null) && GriefPreventionPlus.getInstance().config.claims_preventTheft && ((event.getAction() == Action.RIGHT_CLICK_BLOCK) && (this.isInventoryHolder(clickedBlock) || (clickedBlockType == Material.CAULDRON) || (clickedBlockType == Material.JUKEBOX) || (clickedBlockType == Material.ANVIL) || (clickedBlockType == Material.CAKE_BLOCK) || GriefPreventionPlus.getInstance().config.mods_containerTrustIds.contains(new MaterialInfo(clickedBlock.getType(), clickedBlock.getData(), null))))) {
				if (playerData == null) {
					playerData = this.dataStore.getPlayerData(player.getUniqueId());
				}

				// otherwise check permissions for the claim the player is in
				final Claim claim = this.dataStore.getClaimAt(clickedBlock.getLocation(), false, playerData.lastClaim);
				if (claim != null) {
					final String noContainersReason = claim.canOpenContainers(player);
					if (noContainersReason != null) {
						event.setCancelled(true);
						GriefPreventionPlus.sendMessage(player, TextMode.Err, noContainersReason);
						return;
					}
				}
			}
			// block destroying crops etc while untrusted players jumps on soil
			else if(clickedBlock != null && event.getAction()==Action.PHYSICAL && clickedBlockType==Material.SOIL) {
				if (playerData == null) {
					playerData = this.dataStore.getPlayerData(player.getUniqueId());
				}
				final Claim claim = this.dataStore.getClaimAt(clickedBlock.getLocation(), false, (playerData==null ? null : playerData.lastClaim));
				if (claim!=null) {
					String reason=claim.canBreak(player, clickedBlockType);
					if (reason!=null) {
						player.sendMessage(reason);
						event.setCancelled(true);
					}
				}
				return;
			}

			// otherwise apply rules for doors and beds, if configured that way
			else if (((clickedBlock != null) &&

					(GriefPreventionPlus.getInstance().config.claims_lockWoodenDoors && ((clickedBlockType == Material.WOODEN_DOOR) || (GriefPreventionPlus.isBukkit18 && MC18Helper.isDoor(clickedBlockType))))) ||

			(GriefPreventionPlus.getInstance().config.claims_preventButtonsSwitches && (clickedBlockType == Material.BED_BLOCK)) ||

			(GriefPreventionPlus.getInstance().config.claims_lockTrapDoors && (clickedBlockType == Material.TRAP_DOOR)) ||

			(GriefPreventionPlus.getInstance().config.claims_lockFenceGates && ((clickedBlockType == Material.FENCE_GATE) || (GriefPreventionPlus.isBukkit18 && MC18Helper.isFence(clickedBlockType))))) {
				if (playerData == null) {
					playerData = this.dataStore.getPlayerData(player.getUniqueId());
				}
				final Claim claim = this.dataStore.getClaimAt(clickedBlock.getLocation(), false, playerData.lastClaim);
				if (claim != null) {
					final String noAccessReason = claim.canAccess(player);
					if (noAccessReason != null) {
						event.setCancelled(true);
						GriefPreventionPlus.sendMessage(player, TextMode.Err, noAccessReason);
						return;
					}
				}
			}

			// otherwise apply rules for buttons and switches
			else if ((clickedBlock != null) && GriefPreventionPlus.getInstance().config.claims_preventButtonsSwitches && ((clickedBlockType == null) || (clickedBlockType == Material.STONE_BUTTON) || (clickedBlockType == Material.WOOD_BUTTON) || (clickedBlockType == Material.LEVER) || GriefPreventionPlus.getInstance().config.mods_accessTrustIds.contains(new MaterialInfo(clickedBlock.getType(), clickedBlock.getData(), null)))) {
				if (playerData == null) {
					playerData = this.dataStore.getPlayerData(player.getUniqueId());
				}
				final Claim claim = this.dataStore.getClaimAt(clickedBlock.getLocation(), false, playerData.lastClaim);
				if (claim != null) {
					final String noAccessReason = claim.canAccess(player);
					if (noAccessReason != null) {
						event.setCancelled(true);
						GriefPreventionPlus.sendMessage(player, TextMode.Err, noAccessReason);
						return;
					}
				}
			}

			// otherwise apply rule for cake
			else if ((clickedBlock != null) && GriefPreventionPlus.getInstance().config.claims_preventTheft && (clickedBlockType == Material.CAKE_BLOCK)) {
				if (playerData == null) {
					playerData = this.dataStore.getPlayerData(player.getUniqueId());
				}
				final Claim claim = this.dataStore.getClaimAt(clickedBlock.getLocation(), false, playerData.lastClaim);
				if (claim != null) {
					final String noContainerReason = claim.canAccess(player);
					if (noContainerReason != null) {
						event.setCancelled(true);
						GriefPreventionPlus.sendMessage(player, TextMode.Err, noContainerReason);
						return;
					}
				}
			}

			// apply rule for note blocks and repeaters and daylight sensors
			else if (((clickedBlock != null) && ((clickedBlockType == Material.NOTE_BLOCK) || (clickedBlockType == Material.DIODE_BLOCK_ON) || (clickedBlockType == Material.DIODE_BLOCK_OFF))) || (clickedBlockType == Material.DAYLIGHT_DETECTOR) || (GriefPreventionPlus.isBukkit18 && MC18Helper.isInvDS(clickedBlockType))) {
				if (playerData == null) {
					playerData = this.dataStore.getPlayerData(player.getUniqueId());
				}
				final Claim claim = this.dataStore.getClaimAt(clickedBlock.getLocation(), false, playerData.lastClaim);
				if (claim != null) {
					final String noBuildReason = claim.canBuild(player, clickedBlockType);
					if (noBuildReason != null) {
						event.setCancelled(true);
						GriefPreventionPlus.sendMessage(player, TextMode.Err, noBuildReason);
						return;
					}
				}
			}

			// otherwise handle right click
			else {
				// ignore all actions except right-click on a block or in the air
				if ((action != Action.RIGHT_CLICK_BLOCK) && (action != Action.RIGHT_CLICK_AIR)) {
					return;
				}

				// what's the player holding?
				final ItemStack itemInHand = player.getItemInHand();
				final Material materialInHand = itemInHand.getType();

				// if it's bonemeal or armor stand, check for build permission
				// (ink sac == bone meal, must be a Bukkit bug?)
				if ((clickedBlock != null) && ((materialInHand == Material.INK_SACK) || (GriefPreventionPlus.isBukkit18 && MC18Helper.isArmorStatue(materialInHand)))) {
					final String noBuildReason = GriefPreventionPlus.getInstance().allowBuild(player, clickedBlock.getLocation(), clickedBlockType);
					if (noBuildReason != null) {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, noBuildReason);
						event.setCancelled(true);
					}

					return;
				}

				else if ((clickedBlock != null) && (materialInHand == Material.BOAT)) {
					if (playerData == null) {
						playerData = this.dataStore.getPlayerData(player.getUniqueId());
					}
					final Claim claim = this.dataStore.getClaimAt(clickedBlock.getLocation(), false, playerData.lastClaim);
					if (claim != null) {
						final String noAccessReason = claim.canAccess(player);
						if (noAccessReason != null) {
							GriefPreventionPlus.sendMessage(player, TextMode.Err, noAccessReason);
							event.setCancelled(true);
						}
					}

					return;
				}

				// if it's a spawn egg, minecart, or boat, and this is a
				// creative world, apply special rules
				else if ((clickedBlock != null) && ((materialInHand == Material.MONSTER_EGG) || (materialInHand == Material.MINECART) || (materialInHand == Material.POWERED_MINECART) || (materialInHand == Material.STORAGE_MINECART) || (materialInHand == Material.BOAT)) && GriefPreventionPlus.getInstance().creativeRulesApply(clickedBlock.getWorld())) {
					// player needs build permission at this location
					final String noBuildReason = GriefPreventionPlus.getInstance().allowBuild(player, clickedBlock.getLocation(), Material.MINECART);
					if (noBuildReason != null) {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, noBuildReason);
						event.setCancelled(true);
						return;
					}

					// enforce limit on total number of entities in this claim
					if (playerData == null) {
						playerData = this.dataStore.getPlayerData(player.getUniqueId());
					}
					final Claim claim = this.dataStore.getClaimAt(clickedBlock.getLocation(), false, playerData.lastClaim);
					if (claim == null) {
						return;
					}

					final String noEntitiesReason = claim.allowMoreEntities();
					if (noEntitiesReason != null) {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, noEntitiesReason);
						event.setCancelled(true);
						return;
					}

					return;
				}

			}
		}

		// ignore all actions except right-click on a block or in the air
		if ((action != Action.RIGHT_CLICK_BLOCK) && (action != Action.RIGHT_CLICK_AIR)) {
			return;
		}

		// what's the player holding?
		final ItemStack itemInHand = player.getItemInHand();
		final Material materialInHand = itemInHand.getType();

		if (playerData == null) {
			playerData = this.dataStore.getPlayerData(player.getUniqueId());
		}

		// if he's investigating a claim
		if (materialInHand == GriefPreventionPlus.getInstance().config.claims_investigationTool) {
			// if holding shift (sneaking), show all claims in area
			if (player.isSneaking() && player.hasPermission("griefprevention.visualizenearbyclaims")) {
				// find nearby claims
				final Collection<Claim> claims = this.dataStore.getNearbyClaims(player.getLocation()).values();

				// visualize boundaries
				final Visualization visualization = Visualization.fromClaims(claims, player.getEyeLocation().getBlockY(), VisualizationType.Claim, player.getLocation());
				Visualization.Apply(player, visualization);

				GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.ShowNearbyClaims, String.valueOf(claims.size()));

				event.setCancelled(true);
				return;
			}

			// FEATURE: shovel and stick can be used from a distance away
			if (action == Action.RIGHT_CLICK_AIR) {
				// try to find a far away non-air block along line of sight
				clickedBlock = getTargetBlock(player, 100);
				clickedBlockType = clickedBlock.getType();
			}

			// if no block, stop here
			if (clickedBlock == null) {
				return;
			}

			// air indicates too far away
			if (clickedBlockType == Material.AIR) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.TooFarAway);
				Visualization.Revert(player);
				return;
			}

			Claim claim = this.dataStore.getClaimAt(clickedBlock.getLocation(), false, playerData.lastClaim);

			// no claim case
			if (claim == null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.BlockNotClaimed);
				Visualization.Revert(player);
			}

			// claim case
			else {
				GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.BlockClaimed, claim.getOwnerName());

				// visualize boundary
				final Visualization visualization = Visualization.FromClaim(claim, player.getEyeLocation().getBlockY(), VisualizationType.Claim, player.getLocation());
				Visualization.Apply(player, visualization);

				// if can resize this claim or claiminfo permission, tell about the boundaries
				if (claim.canEdit(player) == null || player.hasPermission("griefprevention.claiminfo")) {
					GriefPreventionPlus.sendMessage(player, TextMode.Info, "ID: " + claim.id + " Area: " + claim.getWidth() + "x" + claim.getHeight() + "=" + claim.getArea() + (claim.getCreationDate() != 0 ? "\nCreated on: " + new Date(claim.getCreationDate()).toString() : ""));
				}

				// if deleteclaims permission or can see claim info permission, tell about the player's offline
				// time
				if (!claim.isAdminClaim() && (player.hasPermission("griefprevention.deleteclaims") || player.hasPermission("griefprevention.claiminfo"))) {
					if (claim.getParent() != null) {
						claim = claim.getParent();
					}
					final PlayerData otherPlayerData = this.dataStore.getPlayerData(claim.getOwnerID());
					if (otherPlayerData.getTimeLastLogin() == 0) {
						otherPlayerData.lastSeen = GriefPreventionPlus.getInstance().getServer().getOfflinePlayer(claim.getOwnerID()).getLastPlayed();
						if (otherPlayerData.lastSeen != 0) {
							this.dataStore.savePlayerData(claim.getOwnerID(), otherPlayerData);
						}
					}
					
					if (otherPlayerData.lastSeen == 0) {
						GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.PlayerOfflineTime, "N/A");
					} else {
						final long daysElapsed = (System.currentTimeMillis() - otherPlayerData.getTimeLastLogin()) / (1000 * 60 * 60 * 24);
						GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.PlayerOfflineTime, String.valueOf(daysElapsed));
					}
				}
				event.setCancelled(true);
			}

			return;
		} else if (materialInHand == GriefPreventionPlus.getInstance().config.claims_modificationTool) {
			// if it's a golden shovel
			// FEATURE: shovel and stick can be used from a distance away
			if (action == Action.RIGHT_CLICK_AIR) {
				// try to find a far away non-air block along line of sight
				clickedBlock = getTargetBlock(player, 100);
				clickedBlockType = clickedBlock.getType();
			}

			// if no block, stop here
			if (clickedBlock == null) {
				return;
			}

			// can't use the shovel from too far away
			if (clickedBlockType == Material.AIR) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.TooFarAway);
				return;
			}

			// if the player is in restore nature mode, do only that
			if ((playerData.shovelMode == ShovelMode.RestoreNature) || (playerData.shovelMode == ShovelMode.RestoreNatureAggressive)) {
				// if the clicked block is in a claim, visualize that claim and
				// deliver an error message
				final Claim claim = this.dataStore.getClaimAt(clickedBlock.getLocation(), false, playerData.lastClaim);
				if (claim != null) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.BlockClaimed, claim.getOwnerName());
					final Visualization visualization = Visualization.FromClaim(claim, clickedBlock.getY(), VisualizationType.ErrorClaim, player.getLocation());
					Visualization.Apply(player, visualization);

					return;
				}

				// figure out which chunk to repair
				final Chunk chunk = player.getWorld().getChunkAt(clickedBlock.getLocation());

				// start the repair process

				// set boundaries for processing
				int miny = clickedBlock.getY();

				// if not in aggressive mode, extend the selection down to a
				// little below sea level
				if (!(playerData.shovelMode == ShovelMode.RestoreNatureAggressive)) {
					if (miny > (GriefPreventionPlus.getInstance().getSeaLevel(chunk.getWorld()) - 10)) {
						miny = GriefPreventionPlus.getInstance().getSeaLevel(chunk.getWorld()) - 10;
					}
				}

				GriefPreventionPlus.getInstance().restoreChunk(chunk, miny, playerData.shovelMode == ShovelMode.RestoreNatureAggressive, 0, player);

				return;
			}

			// if in restore nature fill mode
			if (playerData.shovelMode == ShovelMode.RestoreNatureFill) {
				final ArrayList<Material> allowedFillBlocks = new ArrayList<Material>();
				final Environment environment = clickedBlock.getWorld().getEnvironment();
				if (environment == Environment.NETHER) {
					allowedFillBlocks.add(Material.NETHERRACK);
				} else if (environment == Environment.THE_END) {
					allowedFillBlocks.add(Material.ENDER_STONE);
				} else {
					allowedFillBlocks.add(Material.GRASS);
					allowedFillBlocks.add(Material.DIRT);
					allowedFillBlocks.add(Material.STONE);
					allowedFillBlocks.add(Material.SAND);
					allowedFillBlocks.add(Material.SANDSTONE);
					allowedFillBlocks.add(Material.ICE);
				}

				final Block centerBlock = clickedBlock;

				final int maxHeight = centerBlock.getY();
				final int minx = centerBlock.getX() - playerData.fillRadius;
				final int maxx = centerBlock.getX() + playerData.fillRadius;
				final int minz = centerBlock.getZ() - playerData.fillRadius;
				final int maxz = centerBlock.getZ() + playerData.fillRadius;
				int minHeight = maxHeight - 10;
				if (minHeight < 0) {
					minHeight = 0;
				}

				Claim cachedClaim = null;
				for (int x = minx; x <= maxx; x++) {
					for (int z = minz; z <= maxz; z++) {
						// circular brush
						final Location location = new Location(centerBlock.getWorld(), x, centerBlock.getY(), z);
						if (location.distance(centerBlock.getLocation()) > playerData.fillRadius) {
							continue;
						}

						// default fill block is initially the first from the
						// allowed fill blocks list above
						Material defaultFiller = allowedFillBlocks.get(0);

						// prefer to use the block the player clicked on, if
						// it's an acceptable fill block
						if (allowedFillBlocks.contains(centerBlock.getType())) {
							defaultFiller = centerBlock.getType();
						}

						// if the player clicks on water, try to sink through
						// the water to find something underneath that's useful
						// for a filler
						else if ((centerBlock.getType() == Material.WATER) || (centerBlock.getType() == Material.STATIONARY_WATER)) {
							Block block = centerBlock.getWorld().getBlockAt(centerBlock.getLocation());
							while (!allowedFillBlocks.contains(block.getType()) && (block.getY() > (centerBlock.getY() - 10))) {
								block = block.getRelative(BlockFace.DOWN);
							}
							if (allowedFillBlocks.contains(block.getType())) {
								defaultFiller = block.getType();
							}
						}

						// fill bottom to top
						for (int y = minHeight; y <= maxHeight; y++) {
							final Block block = centerBlock.getWorld().getBlockAt(x, y, z);

							// respect claims
							final Claim claim = this.dataStore.getClaimAt(block.getLocation(), false, cachedClaim);
							if (claim != null) {
								cachedClaim = claim;
								break;
							}

							// only replace air, spilling water, snow, long
							// grass
							if ((block.getType() == Material.AIR) || (block.getType() == Material.SNOW) || ((block.getType() == Material.STATIONARY_WATER) && (block.getData() != 0)) || (block.getType() == Material.LONG_GRASS)) {
								// if the top level, always use the default
								// filler picked above
								if (y == maxHeight) {
									block.setType(defaultFiller);
								}

								// otherwise look to neighbors for an
								// appropriate fill block
								else {
									final Block eastBlock = block.getRelative(BlockFace.EAST);
									final Block westBlock = block.getRelative(BlockFace.WEST);
									final Block northBlock = block.getRelative(BlockFace.NORTH);
									final Block southBlock = block.getRelative(BlockFace.SOUTH);

									// first, check lateral neighbors (ideally,
									// want to keep natural layers)
									if (allowedFillBlocks.contains(eastBlock.getType())) {
										block.setType(eastBlock.getType());
									} else if (allowedFillBlocks.contains(westBlock.getType())) {
										block.setType(westBlock.getType());
									} else if (allowedFillBlocks.contains(northBlock.getType())) {
										block.setType(northBlock.getType());
									} else if (allowedFillBlocks.contains(southBlock.getType())) {
										block.setType(southBlock.getType());
									}

									// if all else fails, use the default filler
									// selected above
									else {
										block.setType(defaultFiller);
									}
								}
							}
						}
					}
				}

				return;
			}

			// if the player doesn't have claims permission, don't do anything
			if (!player.hasPermission("griefprevention.createclaims")) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NoCreateClaimPermission);
				return;
			}

			// if he's resizing a claim and that claim hasn't been deleted since
			// he started resizing it
			if ((playerData.claimResizing != null) && playerData.claimResizing.isInDataStore()) {
				if (clickedBlock.getLocation().equals(playerData.lastShovelLocation)) {
					return;
				}

				// figure out what the coords of his new claim would be
				int newlx, newgx, newlz, newgz;

				if (playerData.claimResizing.lesserX == playerData.claimResizing.greaterX) {
					if (clickedBlock.getX() < playerData.claimResizing.lesserX) {
						newlx = clickedBlock.getX();
						newgx = playerData.claimResizing.greaterX;
					} else {
						newlx = playerData.claimResizing.lesserX;
						newgx = clickedBlock.getX();
					}
				} else {
					if (playerData.lastShovelLocation.getBlockX() == playerData.claimResizing.lesserX) {
						newlx = clickedBlock.getX();
					} else {
						newlx = playerData.claimResizing.lesserX;
					}

					if (playerData.lastShovelLocation.getBlockX() == playerData.claimResizing.greaterX) {
						newgx = clickedBlock.getX();
					} else {
						newgx = playerData.claimResizing.greaterX;
					}
				}

				if (playerData.claimResizing.lesserZ == playerData.claimResizing.greaterZ) {
					if (clickedBlock.getZ() < playerData.claimResizing.lesserZ) {
						newlz = clickedBlock.getZ();
						newgz = playerData.claimResizing.greaterZ;
					} else {
						newlz = playerData.claimResizing.lesserZ;
						newgz = clickedBlock.getZ();
					}
				} else {
					if (playerData.lastShovelLocation.getBlockZ() == playerData.claimResizing.lesserZ) {
						newlz = clickedBlock.getZ();
					} else {
						newlz = playerData.claimResizing.lesserZ;
					}

					if (playerData.lastShovelLocation.getBlockZ() == playerData.claimResizing.greaterZ) {
						newgz = clickedBlock.getZ();
					} else {
						newgz = playerData.claimResizing.greaterZ;
					}
				}

				// be sure lesser corners are always lesser.
				if (newlx > newgx) {
					final int temp = newlx;
					newlx = newgx;
					newgx = temp;
				}
				if (newlz > newgz) {
					final int temp = newlz;
					newlz = newgz;
					newgz = temp;
				}

				// newy1 =
				// playerData.claimResizing.getLesserBoundaryCorner().getBlockY();
				// newy2 = clickedBlock.getY() -
				// GriefPreventionPlus.instance.config.claims_claimsExtendIntoGroundDistance;

				// for top level claims, apply size rules and claim blocks
				// requirement
				if (playerData.claimResizing.getParent() == null) {
					// measure new claim, apply size rules
					final int newWidth = (Math.abs(newlx - newgx) + 1);
					final int newHeight = (Math.abs(newlz - newgz) + 1);
					final boolean smaller = (newWidth < playerData.claimResizing.getWidth()) || (newHeight < playerData.claimResizing.getHeight());

					if (!player.hasPermission("griefprevention.adminclaims") && !playerData.claimResizing.isAdminClaim() && smaller && ((newWidth < GriefPreventionPlus.getInstance().config.claims_minSize) || (newHeight < GriefPreventionPlus.getInstance().config.claims_minSize))) {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.ResizeClaimTooSmall, String.valueOf(GriefPreventionPlus.getInstance().config.claims_minSize));
						return;
					}

					// make sure player has enough blocks to make up the
					// difference
					if (!playerData.claimResizing.isAdminClaim() && player.getUniqueId().equals(playerData.claimResizing.getOwnerID())) {
						final int newArea = newWidth * newHeight;
						final int blocksRemainingAfter = (playerData.getRemainingClaimBlocks() + playerData.claimResizing.getArea()) - newArea;

						if (blocksRemainingAfter < 0) {
							GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.ResizeNeedMoreBlocks, String.valueOf(Math.abs(blocksRemainingAfter)));
							tryAdvertiseAdminAlternatives(player);
							return;
						}
					}
				}

				// special rule for making a top-level claim smaller. to check
				// this, verifying the old claim's corners are inside the new
				// claim's boundaries.
				// rule: in any mode, shrinking a claim removes any surface
				// fluids
				final Claim oldClaim = playerData.claimResizing;
				boolean smaller = false;
				if (oldClaim.getParent() == null) {
					// temporary claim instance, just for checking contains()
					final Claim newClaim = new Claim(oldClaim.getWorldUID(), newlx, newlz, newgx, newgz, null, null, null, null, null);

					// if the new claim is smaller
					if (!newClaim.contains(oldClaim.getLesserBoundaryCorner(), true, false) || !newClaim.contains(oldClaim.getGreaterBoundaryCorner(), true, false)) {
						smaller = true;
						// remove surface fluids about to be unclaimed
						oldClaim.removeSurfaceFluids(newClaim);
					}
				}

				// ask the datastore to try and resize the claim, this checks
				// for conflicts with other claims
				final ClaimResult result = GriefPreventionPlus.getInstance().getDataStore().resizeClaim(playerData.claimResizing, newlx, newlz, newgx, newgz, player);
				switch(result.getResult()) {
					case EVENT: {
						// show the message set by the event
						if (result.getReason()!=null) {
							GriefPreventionPlus.sendMessage(player, TextMode.Err, result.getReason());
						}
						break;
					}
					case OVERLAP: {
						// inform player
						GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.ResizeFailOverlap);
	
						// show the player the conflicting claim
						final Visualization visualization = Visualization.FromClaim(result.getClaim(), clickedBlock.getY(), VisualizationType.ErrorClaim, player.getLocation());
						Visualization.Apply(player, visualization);
						break;
					}
					case WGREGION: {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.ResizeFailOverlapRegion);
						break;
					}
					case SUCCESS: {
						// decide how many claim blocks are available for more
						// resizing
						int claimBlocksRemaining = 0;
						if (!playerData.claimResizing.isAdminClaim()) {
							UUID ownerID = playerData.claimResizing.getOwnerID();
							if (playerData.claimResizing.getParent() != null) {
								ownerID = playerData.claimResizing.getParent().getOwnerID();
							}
							if (ownerID == player.getUniqueId()) {
								claimBlocksRemaining = playerData.getRemainingClaimBlocks();
							} else {
								final PlayerData ownerData = this.dataStore.getPlayerData(ownerID);
								claimBlocksRemaining = ownerData.getRemainingClaimBlocks();
							}
							// inform about success, communicate remaining blocks
							// available
							GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.ClaimResizeSuccess, String.valueOf(claimBlocksRemaining));
						} else {
							GriefPreventionPlus.sendMessage(player, TextMode.Success, "Claim resized.");
						}
	
						// visualize
						final Visualization visualization = Visualization.FromClaim(result.getClaim(), clickedBlock.getY(), VisualizationType.Claim, player.getLocation());
						Visualization.Apply(player, visualization);
	
						// if increased to a sufficiently large size and no
						// subdivisions yet, send subdivision instructions
						if ((oldClaim.getArea() < 1000) && (result.getClaim().getArea() >= 1000) && (result.getClaim().getChildren().size() == 0) && !player.hasPermission("griefprevention.adminclaims")) {
							GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.BecomeMayor, 200L);
							GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.SubdivisionVideo2, 201L, DataStore.SUBDIVISION_VIDEO_URL);
						}
	
						// if in a creative mode world and shrinking an existing
						// claim, restore any unclaimed area
						if (smaller && GriefPreventionPlus.getInstance().creativeRulesApply(oldClaim.getWorld())) {
							GriefPreventionPlus.sendMessage(player, TextMode.Warn, Messages.UnclaimCleanupWarning);
							GriefPreventionPlus.getInstance().restoreClaim(oldClaim, 20L * 60 * 2); // 2
																									// minutes
							GriefPreventionPlus.addLogEntry(player.getName() + " shrank a claim @ " + GriefPreventionPlus.getfriendlyLocationString(playerData.claimResizing.getLesserBoundaryCorner()));
						}
	
						// clean up
						playerData.claimResizing = null;
						playerData.lastShovelLocation = null;
						break;
					}
				}
				return;
			}
			// otherwise, since not currently resizing a claim, must be starting
			// a resize, creating a new claim, or creating a subdivision
			final Claim claim = this.dataStore.getClaimAt(clickedBlock.getLocation(), true, playerData.lastClaim);

			// if within an existing claim, he's not creating a new one
			if (claim != null) {
				// if the player has permission to edit the claim or subdivision
				final String noEditReason = claim.canEdit(player);
				if (noEditReason == null) {
					// if he clicked on a corner, start resizing it
					if (((clickedBlock.getX() == claim.getLesserBoundaryCorner().getBlockX()) || (clickedBlock.getX() == claim.getGreaterBoundaryCorner().getBlockX())) && ((clickedBlock.getZ() == claim.getLesserBoundaryCorner().getBlockZ()) || (clickedBlock.getZ() == claim.getGreaterBoundaryCorner().getBlockZ()))) {
						playerData.claimResizing = claim;

						playerData.lastShovelLocation = clickedBlock.getLocation();
						GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.ResizeStart);
					}

					// if he didn't click on a corner and is in subdivision
					// mode, he's creating a new subdivision
					else if (playerData.shovelMode == ShovelMode.Subdivide) {
						// if it's the first click, he's trying to start a new
						// subdivision
						if (playerData.lastShovelLocation == null) {
							// if the clicked claim was a subdivision, tell him
							// he can't start a new subdivision here
							if (claim.getParent() != null) {
								GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.ResizeFailOverlapSubdivision);
							}

							// otherwise start a new subdivision
							else {
								GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.SubdivisionStart);
								playerData.lastShovelLocation = clickedBlock.getLocation();
								playerData.claimSubdividing = claim;
							}
						}

						// otherwise, he's trying to finish creating a
						// subdivision by setting the other boundary corner
						else {
							// if last shovel location was in a different world,
							// assume the player is starting the create-claim
							// workflow over
							if (!playerData.lastShovelLocation.getWorld().equals(clickedBlock.getWorld())) {
								playerData.lastShovelLocation = null;
								this.onPlayerInteract(event);
								return;
							}

							// try to create a new claim
							final ClaimResult result = this.dataStore.newClaim(player.getWorld().getUID(), playerData.lastShovelLocation.getBlockX(), playerData.lastShovelLocation.getBlockZ(), clickedBlock.getX(), clickedBlock.getZ(), null, playerData.claimSubdividing, null, player);
							
							switch(result.getResult()) {
								case EVENT: {
									// show the message set by the event
									if (result.getReason()!=null) {
										GriefPreventionPlus.sendMessage(player, TextMode.Err, result.getReason());
									}
									break;
								}
								case WGREGION:
								case OVERLAP: {
									GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.CreateSubdivisionOverlap);
	
									final Visualization visualization = Visualization.FromClaim(result.getClaim(), clickedBlock.getY(), VisualizationType.ErrorClaim, player.getLocation());
									Visualization.Apply(player, visualization);
									break;
								}
								case SUCCESS: {
									GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.SubdivisionSuccess);
									final Visualization visualization = Visualization.FromClaim(result.getClaim(), clickedBlock.getY(), VisualizationType.Claim, player.getLocation());
									Visualization.Apply(player, visualization);
									playerData.lastShovelLocation = null;
									playerData.claimSubdividing = null;
									break;
								}
							}
						}
					}

					// otherwise tell him he can't create a claim here, and show
					// him the existing claim
					// also advise him to consider /abandonclaim or resizing the
					// existing claim
					else {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.CreateClaimFailOverlap);
						final Visualization visualization = Visualization.FromClaim(claim, clickedBlock.getY(), VisualizationType.Claim, player.getLocation());
						Visualization.Apply(player, visualization);
						event.setCancelled(true);
					}
				}

				// otherwise tell the player he can't claim here because it's
				// someone else's claim, and show him the claim
				else {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.CreateClaimFailOverlapOtherPlayer, claim.getOwnerName());
					final Visualization visualization = Visualization.FromClaim(claim, clickedBlock.getY(), VisualizationType.ErrorClaim, player.getLocation());
					Visualization.Apply(player, visualization);
					event.setCancelled(true);
				}

				return;
			}

			// otherwise, the player isn't in an existing claim!

			// if he hasn't already start a claim with a previous shovel action
			final Location lastShovelLocation = playerData.lastShovelLocation;
			if (lastShovelLocation == null) {
				// if claims are not enabled in this world and it's not an
				// administrative claim, display an error message and stop
				if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(player.getWorld())) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.ClaimsDisabledWorld);
					return;
				}

				// if he's at the claim count per player limit already and
				// doesn't have permission to bypass, display an error message
				if ((GriefPreventionPlus.getInstance().config.claims_maxClaimsPerPlayer > 0) && !player.hasPermission("griefprevention.overrideclaimcountlimit") && (playerData.getClaims().size() >= GriefPreventionPlus.getInstance().config.claims_maxClaimsPerPlayer)) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.ClaimCreationFailedOverClaimCountLimit);
					return;
				}

				ClaimCreationCornerSetEvent cornerSetEvent = new ClaimCreationCornerSetEvent(player, clickedBlock.getLocation(), true);
				Bukkit.getPluginManager().callEvent(cornerSetEvent);
				if (cornerSetEvent.isCancelled()){
					return;
				}

				// remember it, and start him on the new claim
				playerData.lastShovelLocation = clickedBlock.getLocation();
				GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.ClaimStart);

				// show him where he's working
				final Visualization visualization = Visualization.FromClaim(new Claim(clickedBlock.getLocation(), clickedBlock.getLocation(), null, null, null, null, null), clickedBlock.getY(), VisualizationType.RestoreNature, player.getLocation());
				Visualization.Apply(player, visualization);
			}

			// otherwise, he's trying to finish creating a claim by setting the
			// other boundary corner
			else {
				// if last shovel location was in a different world, assume the
				// player is starting the create-claim workflow over
				if (!lastShovelLocation.getWorld().equals(clickedBlock.getWorld())) {
					playerData.lastShovelLocation = null;
					this.onPlayerInteract(event);
					return;
				}

				ClaimCreationCornerSetEvent cornerSetEvent = new ClaimCreationCornerSetEvent(player, clickedBlock.getLocation(), false);
				Bukkit.getPluginManager().callEvent(cornerSetEvent);
				if (cornerSetEvent.isCancelled()){
					return;
				}

				// apply minimum claim dimensions rule
				final int newClaimWidth = Math.abs(playerData.lastShovelLocation.getBlockX() - clickedBlock.getX()) + 1;
				final int newClaimHeight = Math.abs(playerData.lastShovelLocation.getBlockZ() - clickedBlock.getZ()) + 1;

				if ((playerData.shovelMode != ShovelMode.Admin) && ((newClaimWidth < GriefPreventionPlus.getInstance().config.claims_minSize) || (newClaimHeight < GriefPreventionPlus.getInstance().config.claims_minSize))) {
					// this IF block is a workaround for craftbukkit bug which
					// fires two events for one interaction
					if ((newClaimWidth != 1) && (newClaimHeight != 1)) {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NewClaimTooSmall, String.valueOf(GriefPreventionPlus.getInstance().config.claims_minSize));
					}
					return;
				}
				UUID playerID = player.getUniqueId();

				// if not an administrative claim, verify the player has enough
				// claim blocks for this new claim
				if (playerData.shovelMode != ShovelMode.Admin) {
					final int newClaimArea = newClaimWidth * newClaimHeight;
					final int remainingBlocks = playerData.getRemainingClaimBlocks();
					if (newClaimArea > remainingBlocks) {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.CreateClaimInsufficientBlocks, String.valueOf(newClaimArea - remainingBlocks));
						tryAdvertiseAdminAlternatives(player);
						return;
					}
				} else {
					playerID = GriefPreventionPlus.UUID1;
				}

				// try to create a new claim
				final ClaimResult result = this.dataStore.newClaim(player.getWorld().getUID(), lastShovelLocation.getBlockX(), lastShovelLocation.getBlockZ(), clickedBlock.getX(), clickedBlock.getZ(), playerID, null, null, player);

				switch(result.getResult()) {
					case EVENT: {
						// show the message set by the event
						if (result.getReason()!=null) {
							GriefPreventionPlus.sendMessage(player, TextMode.Err, result.getReason());
						}
						break;
					}
					case OVERLAP: {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.CreateClaimFailOverlapShort);
	
						final Visualization visualization = Visualization.FromClaim(result.getClaim(), clickedBlock.getY(), VisualizationType.ErrorClaim, player.getLocation());
						Visualization.Apply(player, visualization);
						break;
					}
					case SUCCESS: {
						GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.CreateClaimSuccess);
						final Visualization visualization = Visualization.FromClaim(result.getClaim(), clickedBlock.getY(), VisualizationType.Claim, player.getLocation());
						Visualization.Apply(player, visualization);
						playerData.lastShovelLocation = null;
	
						// if it's a big claim, tell the player about subdivisions
						if (!player.hasPermission("griefprevention.adminclaims") && (result.getClaim().getArea() >= 1000)) {
							GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.BecomeMayor, 200L);
							GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.SubdivisionVideo2, 201L, DataStore.SUBDIVISION_VIDEO_URL);
						}
						break;
					}
					case WGREGION: {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.CreateClaimFailOverlapRegion);
						break;
					}
					default: {
						break;
					}
				}
			}
		}
	}

	// when a player successfully joins the server...
	@EventHandler(ignoreCancelled = true, priority = EventPriority.HIGHEST)
	void onPlayerJoin(PlayerJoinEvent event) {
		final Player player = event.getPlayer();
		final UUID playerID = player.getUniqueId();

		// note login time
		final PlayerData playerData = this.dataStore.getPlayerData(playerID);
		playerData.lastSeen = System.currentTimeMillis();
		this.dataStore.savePlayerData(playerID, playerData);
		
		// if player has never played on the server before...
		if (!player.hasPlayedBefore()) {
			// if in survival claims mode, send a message about the claim basics
			// video (except for admins - assumed experts)
			if (GriefPreventionPlus.getInstance().claimsEnabledForWorld(player.getWorld()) && !GriefPreventionPlus.getInstance().config.creativeRulesWorlds.contains(player.getWorld().getName()) && !player.hasPermission("griefprevention.adminclaims") && (this.dataStore.claims.size() > 10)) {
				GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.AvoidGriefClaimLand, 600L);
				GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.SurvivalBasicsVideo2, 601L, DataStore.SURVIVAL_VIDEO_URL);
			}
		}

		// in case player has changed his name, on successful login, update UUID
		// > Name mapping
		GriefPreventionPlus.cacheUUIDNamePair(player.getUniqueId(), player.getName());
	}

	// when a player teleports via a portal
	@EventHandler(ignoreCancelled = true, priority = EventPriority.HIGH)
	void onPlayerPortal(PlayerPortalEvent event) {
		// if the player isn't going anywhere, take no action
		if ((event.getTo() == null) || (event.getTo().getWorld() == null)) {
			return;
		}

		// don't track in worlds where claims are not enabled
		if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(event.getTo().getWorld())) {
			return;
		}

		final Player player = event.getPlayer();

		if (event.getCause() == TeleportCause.NETHER_PORTAL) {
			// FEATURE: when players get trapped in a nether portal, send them
			// back through to the other side

			if (GriefPreventionPlus.getInstance().config.portalTrapSystemEnabled){
				CheckForPortalTrapTask task = new CheckForPortalTrapTask(player, event.getFrom());
				GriefPreventionPlus.getInstance().getServer().getScheduler()
						.scheduleSyncDelayedTask(GriefPreventionPlus.getInstance(), task, 20L * GriefPreventionPlus.getInstance().config.portalTrapSystemDelaySeconds);
			}


			// FEATURE: if the player teleporting doesn't have permission to
			// build a nether portal and none already exists at the destination,
			// cancel the teleportation
			if (GriefPreventionPlus.getInstance().config.claims_portalsRequirePermission) {
				Location destination = event.getTo();
				if (event.useTravelAgent()) {
					if (event.getPortalTravelAgent().getCanCreatePortal()) {
						// hypothetically find where the portal would be created
						// if it were
						final TravelAgent agent = event.getPortalTravelAgent();
						agent.setCanCreatePortal(false);
						destination = agent.findOrCreate(destination);
						agent.setCanCreatePortal(true);
					} else {
						// if not able to create a portal, we don't have to do
						// anything here
						return;
					}
				}

				// if creating a new portal
				if (destination.getBlock().getType() != Material.PORTAL) {
					// check for a land claim and the player's permission that
					// land claim
					final Claim claim = this.dataStore.getClaimAt(destination, false);
					if ((claim != null) && (claim.canBuild(player, Material.PORTAL) != null)) {
						// cancel and inform about the reason
						event.setCancelled(true);
						GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NoBuildPortalPermission, claim.getOwnerName());
					}
				}
			}
		}
	}

	// when a player quits...
	@EventHandler(priority = EventPriority.MONITOR)
	void onPlayerQuit(PlayerQuitEvent event) {
		final Player player = event.getPlayer();
		final UUID playerID = player.getUniqueId();
		final PlayerData playerData = this.dataStore.getPlayerData(playerID);

		// make sure his data is all saved - he might have accrued some claim
		// blocks while playing that were not saved immediately
		
		this.dataStore.savePlayerData(player.getUniqueId(), playerData);
	}

	// when a player spawns, conditionally apply temporary pvp protection
	@EventHandler(ignoreCancelled = true, priority = EventPriority.MONITOR)
	void onPlayerRespawn(PlayerRespawnEvent event) {
		final Player player = event.getPlayer();
		final PlayerData playerData = GriefPreventionPlus.getInstance().getDataStore().getPlayerData(player.getUniqueId());

		// also send him any messaged from grief prevention he would have
		// received while dead
		if (playerData.messageOnRespawn != null) {
			GriefPreventionPlus.sendMessage(player, ChatColor.RESET, playerData.messageOnRespawn, 40L);
			playerData.messageOnRespawn = null;
		}
	}

	static Block getTargetBlock(Player player, int maxDistance) throws IllegalStateException {
		final BlockIterator iterator = new BlockIterator(player.getLocation(), player.getEyeHeight(), maxDistance);
		Block result = player.getLocation().getBlock().getRelative(BlockFace.UP);
		while (iterator.hasNext()) {
			result = iterator.next();
			if ((result.getType() != Material.AIR) && (result.getType() != Material.STATIONARY_WATER) && (result.getType() != Material.LONG_GRASS)) {
				return result;
			}
		}

		return result;
	}

	// educates a player about /adminclaims and /acb, if he can use them
	static void tryAdvertiseAdminAlternatives(Player player) {
		if (player.hasPermission("griefprevention.adminclaims") && player.hasPermission("griefprevention.adjustclaimblocks")) {
			GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.AdvertiseACandACB);
		} else if (player.hasPermission("griefprevention.adminclaims")) {
			GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.AdvertiseAdminClaims);
		} else if (player.hasPermission("griefprevention.adjustclaimblocks")) {
			GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.AdvertiseACB);
		}
	}
}
