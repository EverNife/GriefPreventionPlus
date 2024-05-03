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
import java.util.List;

import net.kaikk.mc.gpp.integration.EverNifeCoreIntegration;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.World.Environment;
import org.bukkit.block.Block;
import org.bukkit.block.BlockFace;
import org.bukkit.block.BlockState;
import org.bukkit.block.Hopper;
import org.bukkit.entity.Item;
import org.bukkit.entity.Player;
import org.bukkit.entity.minecart.HopperMinecart;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.block.BlockBurnEvent;
import org.bukkit.event.block.BlockDispenseEvent;
import org.bukkit.event.block.BlockFromToEvent;
import org.bukkit.event.block.BlockIgniteEvent;
import org.bukkit.event.block.BlockIgniteEvent.IgniteCause;
import org.bukkit.event.block.BlockMultiPlaceEvent;
import org.bukkit.event.block.BlockPistonExtendEvent;
import org.bukkit.event.block.BlockPistonRetractEvent;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.event.block.BlockSpreadEvent;
import org.bukkit.event.inventory.InventoryPickupItemEvent;
import org.bukkit.event.world.StructureGrowEvent;
import org.bukkit.inventory.InventoryHolder;
import org.bukkit.inventory.ItemStack;
import org.bukkit.material.Dispenser;
import org.bukkit.metadata.MetadataValue;

import net.kaikk.mc.gpp.ClaimResult.Result;
import net.kaikk.mc.gpp.visualization.Visualization;
import net.kaikk.mc.gpp.visualization.VisualizationType;

//event handlers related to blocks
@SuppressWarnings("deprecation")
class BlockEventHandler implements Listener {
	// convenience reference to singleton datastore
	private final DataStore dataStore;

	private final ArrayList<Material> trashBlocks;

	// ensures fluids don't flow into land claims from outside
	private Claim lastSpreadClaim = null;

	// constructor
	public BlockEventHandler(DataStore dataStore) {
		this.dataStore = dataStore;

		// create the list of blocks which will not trigger a warning when
		// they're placed outside of land claims
		this.trashBlocks = new ArrayList<Material>();
		this.trashBlocks.add(Material.COBBLESTONE);
		this.trashBlocks.add(Material.TORCH);
		this.trashBlocks.add(Material.DIRT);
		this.trashBlocks.add(Material.SAPLING);
		this.trashBlocks.add(Material.GRAVEL);
		this.trashBlocks.add(Material.SAND);
		this.trashBlocks.add(Material.TNT);
		this.trashBlocks.add(Material.WORKBENCH);
	}

	// when a player breaks a block...
	@EventHandler(priority = EventPriority.LOWEST)
	public void onBlockBreak(BlockBreakEvent breakEvent) {
		final Player player = breakEvent.getPlayer();
		final Block block = breakEvent.getBlock();

		// make sure the player is allowed to break at the location
		final String noBuildReason = GriefPreventionPlus.getInstance().allowBreak(player, block, block.getLocation());
		if (noBuildReason != null) {
			GriefPreventionPlus.sendMessage(player, TextMode.Err, noBuildReason);
			breakEvent.setCancelled(true);
			return;
		}
	}

	// blocks are not destroyed by fire, unless configured to do so
	@EventHandler(priority = EventPriority.LOWEST)
	public void onBlockBurn(BlockBurnEvent burnEvent) {
		// don't track in worlds where claims are not enabled
		if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(burnEvent.getBlock().getWorld())) {
			return;
		}

		if (!GriefPreventionPlus.getInstance().config.fireDestroys) {
			burnEvent.setCancelled(true);
			final Block block = burnEvent.getBlock();
			final Block[] adjacentBlocks = new Block[] { block.getRelative(BlockFace.UP), block.getRelative(BlockFace.DOWN), block.getRelative(BlockFace.NORTH), block.getRelative(BlockFace.SOUTH), block.getRelative(BlockFace.EAST), block.getRelative(BlockFace.WEST) };

			// pro-actively put out any fires adjacent the burning block, to
			// reduce future processing here
			for (int i = 0; i < adjacentBlocks.length; i++) {
				final Block adjacentBlock = adjacentBlocks[i];
				if ((adjacentBlock.getType() == Material.FIRE) && (adjacentBlock.getRelative(BlockFace.DOWN).getType() != Material.NETHERRACK)) {
					adjacentBlock.setType(Material.AIR);
				}
			}

			final Block aboveBlock = block.getRelative(BlockFace.UP);
			if (aboveBlock.getType() == Material.FIRE) {
				aboveBlock.setType(Material.AIR);
			}
			return;
		}

		// never burn claimed blocks, regardless of settings
		if (this.dataStore.getClaimAt(burnEvent.getBlock().getLocation(), false) != null) {
			burnEvent.setCancelled(true);
		}
	}

	@EventHandler(priority = EventPriority.LOWEST)
	public void onBlockFromTo(BlockFromToEvent spreadEvent) {
		if (spreadEvent == null) {
			return;
		}

		// always allow fluids to flow straight down or up (Thermal Expansion's
		// Energized Glowstone)
		if ((spreadEvent.getFace() == BlockFace.DOWN) || (spreadEvent.getFace() == BlockFace.UP)) {
			return;
		}

		// don't track in worlds where claims are not enabled
		if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(spreadEvent.getBlock().getWorld())) {
			return;
		}

		// where to?
		final Block toBlock = spreadEvent.getToBlock();
		final Location toLocation = toBlock.getLocation();
		final Claim toClaim = this.dataStore.getClaimAt(toLocation, false, this.lastSpreadClaim);

		// if into a land claim, it must be from the same land claim
		if (toClaim != null) {
			this.lastSpreadClaim = toClaim;
			if (!toClaim.contains(spreadEvent.getBlock().getLocation(), false, true)) {
				// exception: from parent into subdivision
				if ((toClaim.getParent() == null) || !toClaim.getParent().contains(spreadEvent.getBlock().getLocation(), false, false)) {
					spreadEvent.setCancelled(true);
				}
			}
		}
	}

	// blocks are ignited ONLY by flint and steel (not by being near lava, open
	// flames, etc), unless configured otherwise
	@EventHandler(priority = EventPriority.LOWEST)
	public void onBlockIgnite(BlockIgniteEvent igniteEvent) {
		// don't track in worlds where claims are not enabled
		if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(igniteEvent.getBlock().getWorld())) {
			return;
		}

		if (!GriefPreventionPlus.getInstance().config.fireSpreads && (igniteEvent.getCause() != IgniteCause.FLINT_AND_STEEL) && (igniteEvent.getCause() != IgniteCause.LIGHTNING)) {
			igniteEvent.setCancelled(true);
		}
	}

	// blocks "pushing" other players' blocks around (pistons)
	@EventHandler(priority = EventPriority.LOWEST)
	public void onBlockPistonExtend(BlockPistonExtendEvent event) {
		// pushing down is ALWAYS safe
		if (event.getDirection() == BlockFace.DOWN) {
			return;
		}

		// don't track in worlds where claims are not enabled
		if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(event.getBlock().getWorld())) {
			return;
		}

		final Block pistonBlock = event.getBlock();
		final List<Block> blocks = event.getBlocks();

		// if no blocks moving, then only check to make sure we're not pushing
		// into a claim from outside
		// this avoids pistons breaking non-solids just inside a claim, like
		// torches, doors, and touchplates
		if (blocks.size() == 0) {
			final Block invadedBlock = pistonBlock.getRelative(event.getDirection());

			// pushing "air" is harmless
			if (invadedBlock.getType() == Material.AIR) {
				return;
			}

			if ((this.dataStore.getClaimAt(pistonBlock.getLocation(), false) == null) && (this.dataStore.getClaimAt(invadedBlock.getLocation(), false) != null)) {
				event.setCancelled(true);
			}

			return;
		}

		// who owns the piston, if anyone?
		String pistonClaimOwnerName = "_";
		Claim claim = this.dataStore.getClaimAt(event.getBlock().getLocation(), false);
		if (claim != null) {
			pistonClaimOwnerName = claim.getOwnerName();
		}

		// if pistons are limited to same-claim block movement
		if (GriefPreventionPlus.getInstance().config.pistonsInClaimsOnly) {
			// if piston is not in a land claim, cancel event
			if (claim == null) {
				event.setCancelled(true);
				return;
			}

			for (final Block pushedBlock : event.getBlocks()) {
				// if pushing blocks located outside the land claim it lives in,
				// cancel the event
				if (!claim.contains(pushedBlock.getLocation(), false, false)) {
					event.setCancelled(true);
					return;
				}

				// if pushing a block inside the claim out of the claim, cancel
				// the event
				// reason: could push into another land claim, don't want to
				// spend CPU checking for that
				// reason: push ice out, place torch, get water outside the
				// claim
				if (!claim.contains(pushedBlock.getRelative(event.getDirection()).getLocation(), false, false)) {
					event.setCancelled(true);
					return;
				}
			}
		}

		// otherwise, consider ownership of piston and EACH pushed block
		else {
			// which blocks are being pushed?
			Claim cachedClaim = claim;
			for (int i = 0; i < blocks.size(); i++) {
				// if ANY of the pushed blocks are owned by someone other than
				// the piston owner, cancel the event
				final Block block = blocks.get(i);
				claim = this.dataStore.getClaimAt(block.getLocation(), false, cachedClaim);
				if (claim != null) {
					cachedClaim = claim;
					if (!claim.getOwnerName().equals(pistonClaimOwnerName)) {
						event.setCancelled(true);
						event.getBlock().getWorld().createExplosion(event.getBlock().getLocation(), 0);
						event.getBlock().getWorld().dropItem(event.getBlock().getLocation(), new ItemStack(event.getBlock().getType()));
						event.getBlock().setType(Material.AIR);
						return;
					}
				}
			}

			// if any of the blocks are being pushed into a claim from outside,
			// cancel the event
			for (int i = 0; i < blocks.size(); i++) {
				final Block block = blocks.get(i);
				final Claim originalClaim = this.dataStore.getClaimAt(block.getLocation(), false, cachedClaim);
				String originalOwnerName = "";
				if (originalClaim != null) {
					cachedClaim = originalClaim;
					originalOwnerName = originalClaim.getOwnerName();
				}

				final Claim newClaim = this.dataStore.getClaimAt(block.getRelative(event.getDirection()).getLocation(), false, cachedClaim);
				String newOwnerName = "";
				if (newClaim != null) {
					newOwnerName = newClaim.getOwnerName();
				}

				// if pushing this block will change ownership, cancel the event
				// and take away the piston (for performance reasons)
				if (!newOwnerName.equals(originalOwnerName) && !newOwnerName.isEmpty()) {
					event.setCancelled(true);
					event.getBlock().getWorld().createExplosion(event.getBlock().getLocation(), 0);
					event.getBlock().getWorld().dropItem(event.getBlock().getLocation(), new ItemStack(event.getBlock().getType()));
					event.getBlock().setType(Material.AIR);
					return;
				}
			}
		}
	}

	// blocks theft by pulling blocks out of a claim (again pistons)
	@EventHandler(priority = EventPriority.LOWEST)
	public void onBlockPistonRetract(BlockPistonRetractEvent event) {
		// if MC1.8, EventHandler18 will handle this
		if (GriefPreventionPlus.isBukkit18) {
			return;
		}

		// we only care about sticky pistons retracting
		if (!event.isSticky()) {
			return;
		}

		// pulling up is always safe
		if (event.getDirection() == BlockFace.UP) {
			return;
		}

		// if pulling "air", always safe
		if (event.getRetractLocation().getBlock().getType() == Material.AIR) {
			return;
		}

		// don't track in worlds where claims are not enabled
		if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(event.getBlock().getWorld())) {
			return;
		}

		// if pistons limited to only pulling blocks which are in the same claim
		// the piston is in
		if (GriefPreventionPlus.getInstance().config.pistonsInClaimsOnly) {
			// if piston not in a land claim, cancel event
			final Claim pistonClaim = this.dataStore.getClaimAt(event.getBlock().getLocation(), false);
			if (pistonClaim == null) {
				event.setCancelled(true);
				return;
			}

			// if pulled block isn't in the same land claim, cancel the event
			if (!pistonClaim.contains(event.getRetractLocation(), false, false)) {
				event.setCancelled(true);
				return;
			}
		}

		// otherwise, consider ownership of both piston and block
		else {
			// who owns the moving block, if anyone?
			String movingBlockOwnerName = "_";
			final Claim movingBlockClaim = this.dataStore.getClaimAt(event.getRetractLocation(), false);
			if (movingBlockClaim != null) {
				movingBlockOwnerName = movingBlockClaim.getOwnerName();
			}

			// who owns the piston, if anyone?
			String pistonOwnerName = "_";
			final Location pistonLocation = event.getBlock().getLocation();
			final Claim pistonClaim = this.dataStore.getClaimAt(pistonLocation, false, movingBlockClaim);
			if (pistonClaim != null) {
				pistonOwnerName = pistonClaim.getOwnerName();
			}

			// if there are owners for the blocks, they must be the same player
			// otherwise cancel the event
			if (!pistonOwnerName.equals(movingBlockOwnerName)) {
				event.setCancelled(true);
			}
		}
	}

	// when a player places a block...
	@EventHandler(priority = EventPriority.LOW)
	public void onBlockPlace(BlockPlaceEvent placeEvent) {
		final Player player = placeEvent.getPlayer();		
		final Block block = placeEvent.getBlock();

		if (GriefPreventionPlus.getInstance().config.mods_ignoreAllFakePlayers && EverNifeCoreIntegration.isFakePlayer(player)){
			return;
		}

		// FEATURE: limit fire placement, to prevent PvP-by-fire

		// if placed block is fire and pvp is off, apply rules for proximity to
		// other players
		if ((block.getType() == Material.FIRE) && !GriefPreventionPlus.getInstance().config.pvp_enabledWorlds.contains(block.getWorld().getUID()) && !player.hasPermission("griefprevention.lava")) {
			final List<Player> players = block.getWorld().getPlayers();
			for (int i = 0; i < players.size(); i++) {
				final Player otherPlayer = players.get(i);
				final Location location = otherPlayer.getLocation();
				if (!otherPlayer.equals(player) && (location.distanceSquared(block.getLocation()) < 9)) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.PlayerTooCloseForFire, otherPlayer.getName());
					placeEvent.setCancelled(true);

					player.getWorld().dropItemNaturally(player.getLocation(), player.getItemInHand());
					player.setItemInHand(null);
					return;
				}
			}
		}

		// don't track in worlds where claims are not enabled
		if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(placeEvent.getBlock().getWorld())) {
			return;
		}

		// make sure the player is allowed to build at the location
		final String noBuildReason = GriefPreventionPlus.getInstance().allowBuild(player, block.getLocation(), block.getType());
		if (noBuildReason != null) {
			GriefPreventionPlus.sendMessage(player, TextMode.Err, noBuildReason);
			placeEvent.setCancelled(true);
			return;
		}

		// if the block is being placed within or under an existing claim
		final PlayerData playerData = this.dataStore.getPlayerData(player.getUniqueId());
		final Claim claim = this.dataStore.getClaimAt(block.getLocation(), false, playerData.lastClaim);
		if (claim != null) {
			// warn about TNT not destroying claimed blocks
			if ((block.getType() == Material.TNT) && !claim.areExplosivesAllowed()) {
				GriefPreventionPlus.sendMessage(player, TextMode.Warn, Messages.NoTNTDamageClaims);
				GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.ClaimExplosivesAdvertisement);
			}

			// allow for a build warning in the future
			playerData.warnedAboutBuildingOutsideClaims = false;
		}

		// FEATURE: automatically create a claim when a player who has no claims places a chest
		// otherwise if there's no claim, the player is placing a chest, and new
		// player automatic claims are enabled
		else if ((block.getType() == Material.CHEST) && (GriefPreventionPlus.getInstance().config.claims_automaticClaimsForNewPlayersRadius > -1) && GriefPreventionPlus.getInstance().claimsEnabledForWorld(block.getWorld())) {
			// ignore fake players
			if (Utils.isFakePlayer(player)) {
				return;
			}
			
			// if the chest is too deep underground, don't create the claim and
			// explain why
			if (GriefPreventionPlus.getInstance().config.claims_preventTheft && (block.getY() < GriefPreventionPlus.getInstance().config.claims_maxDepth)) {
				GriefPreventionPlus.sendMessage(player, TextMode.Warn, Messages.TooDeepToClaim);
				return;
			}

			int radius = GriefPreventionPlus.getInstance().config.claims_automaticClaimsForNewPlayersRadius;

			// if the player doesn't have any claims yet, automatically create a
			// claim centered at the chest
			if (playerData.getClaims().size() == 0) {
				// radius == 0 means protect ONLY the chest
				if (GriefPreventionPlus.getInstance().config.claims_automaticClaimsForNewPlayersRadius == 0) {
					this.dataStore.newClaim(block.getWorld().getUID(), block.getX(), block.getZ(), block.getX(), block.getZ(), player.getUniqueId(), null, null, player);
					GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.ChestClaimConfirmation);
				}

				// otherwise, create a claim in the area around the chest
				else {
					// as long as the automatic claim overlaps another existing
					// claim, shrink it
					// note that since the player had permission to place the
					// chest, at the very least, the automatic claim will
					// include the chest
					while ((radius >= 0) && this.dataStore.newClaim(block.getWorld().getUID(), block.getX() - radius, block.getZ() - radius, block.getX() + radius, block.getZ() + radius, player.getUniqueId(), null, null, player).getResult()!=Result.SUCCESS) {
						radius--;
					}

					// notify and explain to player
					GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.AutomaticClaimNotification);

					// show the player the protected area
					final Claim newClaim = this.dataStore.getClaimAt(block.getLocation());
					final Visualization visualization = Visualization.FromClaim(newClaim, block.getY(), VisualizationType.Claim, player.getLocation());
					Visualization.Apply(player, visualization);
				}

				GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.SurvivalBasicsVideo2, DataStore.SURVIVAL_VIDEO_URL);
			}

			// check to see if this chest is in a claim, and warn when it isn't
			if (GriefPreventionPlus.getInstance().config.claims_preventTheft && (this.dataStore.getClaimAt(block.getLocation(), false, playerData.lastClaim) == null)) {
				GriefPreventionPlus.sendMessage(player, TextMode.Warn, Messages.UnprotectedChestWarning);
			}
		}

		// FEATURE: limit wilderness tree planting to grass, or dirt with more
		// blocks beneath it
		else if ((block.getType() == Material.SAPLING) && GriefPreventionPlus.getInstance().config.blockSkyTrees && GriefPreventionPlus.getInstance().claimsEnabledForWorld(player.getWorld())) {
			final Block earthBlock = placeEvent.getBlockAgainst();
			if (earthBlock.getType() != Material.GRASS) {
				if ((earthBlock.getRelative(BlockFace.DOWN).getType() == Material.AIR) || (earthBlock.getRelative(BlockFace.DOWN).getRelative(BlockFace.DOWN).getType() == Material.AIR)) {
					placeEvent.setCancelled(true);
				}
			}
		}

		// FEATURE: warn players when they're placing non-trash blocks outside
		// of their claimed areas
		else if (!this.trashBlocks.contains(block.getType()) && GriefPreventionPlus.getInstance().claimsEnabledForWorld(block.getWorld())) {
			if (!playerData.warnedAboutBuildingOutsideClaims && !player.hasPermission("griefprevention.adminclaims") && (((playerData.lastClaim == null) && (playerData.getClaims().size() == 0)) || ((playerData.lastClaim != null) && playerData.lastClaim.isNear(player.getLocation(), 15)))) {
				final Long now = System.currentTimeMillis();
				if ((playerData.buildWarningTimestamp == null) || ((now - playerData.buildWarningTimestamp) > 600000)) // 10
																														// minute
																														// cooldown
				{
					GriefPreventionPlus.sendMessage(player, TextMode.Warn, Messages.BuildingOutsideClaims);
					playerData.warnedAboutBuildingOutsideClaims = true;

					playerData.buildWarningTimestamp = now;

					if (playerData.getClaims().size() < 2) {
						GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.SurvivalBasicsVideo2, DataStore.SURVIVAL_VIDEO_URL);
					}

					if (playerData.lastClaim != null) {
						final Visualization visualization = Visualization.FromClaim(playerData.lastClaim, block.getY(), VisualizationType.Claim, player.getLocation());
						Visualization.Apply(player, visualization);
					}
				}
			}
		}

		// warn players when they place TNT above sea level, since it doesn't
		// destroy blocks there
		if (GriefPreventionPlus.getInstance().config.blockSurfaceOtherExplosions && (block.getType() == Material.TNT) && (block.getWorld().getEnvironment() != Environment.NETHER) && (block.getY() > (GriefPreventionPlus.getInstance().getSeaLevel(block.getWorld()) - 5)) && (claim == null)) {
			GriefPreventionPlus.sendMessage(player, TextMode.Warn, Messages.NoTNTDamageAboveSeaLevel);
		}

		// warn players about disabled pistons outside of land claims
		if (GriefPreventionPlus.getInstance().config.pistonsInClaimsOnly && ((block.getType() == Material.PISTON_BASE) || (block.getType() == Material.PISTON_STICKY_BASE)) && (claim == null)) {
			GriefPreventionPlus.sendMessage(player, TextMode.Warn, Messages.NoPistonsOutsideClaims);
		}
	}

	// when a player places multiple blocks...
	@EventHandler(priority = EventPriority.LOW)
	public void onBlocksPlace(BlockMultiPlaceEvent placeEvent) {
		final Player player = placeEvent.getPlayer();

		if (GriefPreventionPlus.getInstance().config.mods_ignoreAllFakePlayers && EverNifeCoreIntegration.isFakePlayer(player)){
			return;
		}

		// don't track in worlds where claims are not enabled
		if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(placeEvent.getBlock().getWorld())) {
			return;
		}

		// make sure the player is allowed to build at the location
		for (final BlockState block : placeEvent.getReplacedBlockStates()) {
			final String noBuildReason = GriefPreventionPlus.getInstance().allowBuild(player, block.getLocation(), block.getType());
			if (noBuildReason != null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, noBuildReason);
				placeEvent.setCancelled(true);
				return;
			}
		}
	}

	// fire doesn't spread unless configured to, but other blocks still do
	// (mushrooms and vines, for example)
	@EventHandler(priority = EventPriority.LOWEST)
	public void onBlockSpread(BlockSpreadEvent spreadEvent) {
		if (spreadEvent.getSource().getType() != Material.FIRE) {
			return;
		}

		// don't track in worlds where claims are not enabled
		if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(spreadEvent.getBlock().getWorld())) {
			return;
		}

		if (!GriefPreventionPlus.getInstance().config.fireSpreads) {
			spreadEvent.setCancelled(true);

			final Block underBlock = spreadEvent.getSource().getRelative(BlockFace.DOWN);
			if (underBlock.getType() != Material.NETHERRACK) {
				spreadEvent.getSource().setType(Material.AIR);
			}

			return;
		}

		// never spread into a claimed area, regardless of settings
		if (this.dataStore.getClaimAt(spreadEvent.getBlock().getLocation(), false) != null) {
			spreadEvent.setCancelled(true);

			// if the source of the spread is not fire on netherrack, put out
			// that source fire to save cpu cycles
			final Block source = spreadEvent.getSource();
			if (source.getRelative(BlockFace.DOWN).getType() != Material.NETHERRACK) {
				source.setType(Material.AIR);
			}
		}
	}

	// ensures dispensers can't be used to dispense a block(like water or lava)
	// or item across a claim boundary
	@EventHandler(priority = EventPriority.LOWEST)
	public void onDispense(BlockDispenseEvent dispenseEvent) {
		// don't track in worlds where claims are not enabled
		if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(dispenseEvent.getBlock().getWorld())) {
			return;
		}

		// from where?
		final Block fromBlock = dispenseEvent.getBlock();
		final Dispenser dispenser = new Dispenser(fromBlock.getType(), fromBlock.getData());

		// to where?
		final Block toBlock = fromBlock.getRelative(dispenser.getFacing());
		final Claim fromClaim = this.dataStore.getClaimAt(fromBlock.getLocation(), false);
		final Claim toClaim = this.dataStore.getClaimAt(toBlock.getLocation(), false, fromClaim);

		// into wilderness is NOT OK in creative mode worlds
		final Material materialDispensed = dispenseEvent.getItem().getType();
		if (((materialDispensed == Material.WATER_BUCKET) || (materialDispensed == Material.LAVA_BUCKET)) && GriefPreventionPlus.getInstance().creativeRulesApply(dispenseEvent.getBlock().getWorld()) && (toClaim == null)) {
			dispenseEvent.setCancelled(true);
			return;
		}

		// wilderness to wilderness is OK
		if ((fromClaim == null) && (toClaim == null)) {
			return;
		}

		// within claim is OK
		if (fromClaim == toClaim) {
			return;
		}

		// everything else is NOT OK
		dispenseEvent.setCancelled(true);
	}

	@EventHandler
	public void onInventoryPickupItem(InventoryPickupItemEvent event) {
		// prevent hoppers from picking-up items dropped by players on death

		final InventoryHolder holder = event.getInventory().getHolder();
		if ((holder instanceof HopperMinecart) || (holder instanceof Hopper)) {
			final Item item = event.getItem();
			final List<MetadataValue> data = item.getMetadata("GP_ITEMOWNER");

			// if this is marked as belonging to a player
			if ((data != null) && (data.size() > 0)) {
				// don't allow the pickup
				event.setCancelled(true);
			}
		}
	}

	@EventHandler
	public void onTreeGrow(StructureGrowEvent growEvent) {
		// only take these potentially expensive steps if configured to do so
		if (!GriefPreventionPlus.getInstance().config.limitTreeGrowth) {
			return;
		}

		// don't track in worlds where claims are not enabled
		if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(growEvent.getWorld())) {
			return;
		}

		final Location rootLocation = growEvent.getLocation();
		Claim rootClaim = this.dataStore.getClaimAt(rootLocation, false);
		String rootOwnerName = null;

		// who owns the spreading block, if anyone?
		if (rootClaim != null) {
			// tree growth in subdivisions is dependent on who owns the top
			// level claim
			if (rootClaim.getParent() != null) {
				rootClaim = rootClaim.getParent();
			}

			// if an administrative claim, just let the tree grow where it wants
			if (rootClaim.isAdminClaim()) {
				return;
			}

			// otherwise, note the owner of the claim
			rootOwnerName = rootClaim.getOwnerName();
		}

		// for each block growing
		for (int i = 0; i < growEvent.getBlocks().size(); i++) {
			final BlockState block = growEvent.getBlocks().get(i);
			final Claim blockClaim = this.dataStore.getClaimAt(block.getLocation(), false, rootClaim);

			// if it's growing into a claim
			if (blockClaim != null) {
				// if there's no owner for the new tree, or the owner for the
				// new tree is different from the owner of the claim
				if ((rootOwnerName == null) || !rootOwnerName.equals(blockClaim.getOwnerName())) {
					growEvent.getBlocks().remove(i--);
				}
			}
		}
	}
}
