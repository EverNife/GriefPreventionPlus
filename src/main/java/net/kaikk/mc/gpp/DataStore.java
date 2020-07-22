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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.bukkit.ChatColor;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.entity.Player;

import net.kaikk.mc.gpp.ClaimResult.Result;
import net.kaikk.mc.gpp.events.ClaimCreateEvent;
import net.kaikk.mc.gpp.events.ClaimDeleteEvent;
import net.kaikk.mc.gpp.events.ClaimDeleteEvent.Reason;
import net.kaikk.mc.gpp.events.ClaimResizeEvent;

//singleton class which manages all GriefPrevention data (except for config options)
public abstract class DataStore {
	// in-memory cache for player data
	protected Map<UUID, PlayerData> playersData = new HashMap<UUID, PlayerData>();

	// in-memory cache for group (permission-based) data
	protected Map<String, Integer> permissionToBonusBlocksMap = new HashMap<String, Integer>();

	// in-memory cache for claim data
	public Map<Integer, Claim> claims = new HashMap<Integer, Claim>();
	public Map<Integer, Map<Integer, Claim>> posClaims = new HashMap<Integer, Map<Integer, Claim>>();

	// in-memory cache for messages
	public String[] messages;

	// pattern for unique user identifiers (UUIDs)
	// protected final static Pattern uuidpattern =
	// Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

	// world guard reference, if available
	protected WorldGuardWrapper worldGuard = null;

	public DataStore() {
	}

	// initialization!
	void initialize() throws Exception {

	}
	
	/** grants a group (players with a specific permission) bonus claim blocks as
	    long as they're still members of the group */
	public int adjustGroupBonusBlocks(String groupName, int amount) {
		Integer currentValue = this.permissionToBonusBlocksMap.get(groupName);
		if (currentValue == null) {
			currentValue = 0;
		}

		currentValue += amount;
		this.permissionToBonusBlocksMap.put(groupName, currentValue);

		// write changes to storage to ensure they don't get lost
		this.saveGroupBonusBlocks(groupName, currentValue);

		return currentValue;
	}

	/** saves changes to player data. MUST be called after you're done making
	    changes, otherwise a reload will lose them */
	public abstract void asyncSavePlayerData(UUID playerID, PlayerData playerData);

	/** changes the claim owner
	 *  @throws Exception if the claim is a subdivision
	 * */
	public void changeClaimOwner(Claim claim, UUID newOwnerID) throws Exception {
		// if it's a subdivision, throw an exception
		if (claim.getParent() != null) {
			throw new Exception("Subdivisions can't be transferred.  Only top-level claims may change owners.");
		}

		// otherwise update information

		// determine current claim owner
		PlayerData ownerData = null;
		if (!claim.isAdminClaim()) {
			ownerData = this.getPlayerData(claim.getOwnerID());
		}

		// determine new owner
		PlayerData newOwnerData = null;

		if (newOwnerID != null) {
			newOwnerData = this.getPlayerData(newOwnerID);
		}

		// transfer
		claim.setOwnerID(newOwnerID);
		this.dbUpdateOwner(claim);

		// adjust blocks and other records
		if (ownerData != null) {
			ownerData.getClaims().remove(claim);
		}

		if (newOwnerData != null) {
			newOwnerData.getClaims().add(claim);
		}
	}
	
	
	/**
	 * Clears all permissions (/trust, etc..) on all claims owned by the specified player
	 * */
	public void clearPermissionsOnPlayerClaims(UUID ownerId) {
		final PlayerData ownerData = this.getPlayerData(ownerId);
		if (ownerData != null) {
			this.dbUnsetPerm(ownerId);
			for (final Claim c : ownerData.getClaims()) {
				c.clearMemoryPermissions();
			}
		}
	}

	/** @deprecated Use {@link #newClaim(UUID,int,int,int,int,UUID,Claim,Integer,Player)} instead*/
	public ClaimResult createClaim(UUID world, int x1, int x2, int z1, int z2, UUID ownerID, Claim parent, Integer id, Player creatingPlayer) {
		return newClaim(world, x1, z1, x2, z2, ownerID, parent, id, creatingPlayer);
	}

	/** creates a claim.
	if the new claim would overlap an existing claim, returns a failure along
	with a reference to the existing claim
	if the new claim would overlap a WorldGuard region where the player
	doesn't have permission to build, returns a failure with NULL for claim
	otherwise, returns a success along with a reference to the new claim
	use ownerName == "" for administrative claims
	for top level claims, pass parent == NULL
	- DOES adjust claim blocks available on success (players can go into
	negative quantity available)
	- DOES check for world guard regions where the player doesn't have
	permission
	- does NOT check a player has permission to create a claim, or enough claim
	blocks.
	- does NOT check minimum claim size constraints
	- does NOT visualize the new claim for any players */
	public ClaimResult newClaim(UUID world, int x1, int z1, int x2, int z2, UUID ownerID, Claim parent, Integer id, Player creatingPlayer) {
		final ClaimResult result = new ClaimResult();

		int smallx, bigx, smallz, bigz;

		// determine small versus big inputs
		if (x1 < x2) {
			smallx = x1;
			bigx = x2;
		} else {
			smallx = x2;
			bigx = x1;
		}

		if (z1 < z2) {
			smallz = z1;
			bigz = z2;
		} else {
			smallz = z2;
			bigz = z1;
		}

		// create a new claim instance (but don't save it, yet)
		final Claim newClaim = new Claim(world, smallx, smallz, bigx, bigz, ownerID, null, null, null, id);

		newClaim.setParent(parent);

		// ensure this new claim won't overlap any existing claims
		ArrayList<Claim> claimsToCheck;
		if (newClaim.getParent() != null) {
			claimsToCheck = newClaim.getParent().getChildren();
		} else {
			claimsToCheck = new ArrayList<Claim>(this.claims.values());
		}

		for (int i = 0; i < claimsToCheck.size(); i++) {
			final Claim otherClaim = claimsToCheck.get(i);

			// if we find an existing claim which will be overlapped
			if (otherClaim.overlaps(newClaim)) {
				// result = fail, return conflicting claim
				result.setResult(Result.OVERLAP);
				result.setClaim(otherClaim);
				return result;
			}
		}

		// if worldguard is installed, also prevent claims from overlapping any
		// worldguard regions
		if (GriefPreventionPlus.getInstance().config.claims_respectWorldGuard && (this.worldGuard != null) && (creatingPlayer != null)) {
			if (!this.worldGuard.canBuild(newClaim.getWorld(), newClaim.lesserX, newClaim.lesserZ, newClaim.greaterX, newClaim.greaterZ, creatingPlayer)) {
				result.setResult(Result.WGREGION);
				return result;
			}
		}
		
		// call the event
		ClaimCreateEvent event = new ClaimCreateEvent(newClaim, creatingPlayer);
		GriefPreventionPlus.getInstance().getServer().getPluginManager().callEvent(event);
		if (event.isCancelled()) {
			result.setResult(Result.EVENT);
			result.setReason(event.getReason());
			return result;
		}
		
		// otherwise add this new claim to the data store to make it effective
		this.addClaim(newClaim, true);

		// then return success along with reference to new claim
		result.setResult(Result.SUCCESS);
		result.setClaim(newClaim);
		return result;
	}

	/** deletes a claim or subdivision */
	public void deleteClaim(Claim claim) {
		if (claim.getParent() != null) { // subdivision
			final Claim parentClaim = claim.getParent();
			parentClaim.getChildren().remove(claim);
			this.deleteClaimFromSecondaryStorage(claim);
			return;
		}

		// remove from memory
		this.posClaimsRemove(claim);
		this.claims.remove(claim.id);

		// remove from secondary storage
		this.deleteClaimFromSecondaryStorage(claim);

		// update player data, except for administrative claims, which have no
		// owner
		if (!claim.isAdminClaim()) {
			final PlayerData ownerData = this.getPlayerData(claim.getOwnerID());
			for (int i = 0; i < ownerData.getClaims().size(); i++) {
				if (ownerData.getClaims().get(i).id == claim.id) {
					ownerData.getClaims().remove(i);
					break;
				}
			}
			this.savePlayerData(claim.getOwnerID(), ownerData);
		}
	}

	/** deletes all claims owned by a player */
	public void deleteClaimsForPlayer(UUID claimsOwner, Player sender, boolean deleteCreativeClaims) {
		List<Claim> claimsToRemove = new ArrayList<Claim>();
		for (final Claim claim : this.claims.values()) {
			if (claimsOwner.equals(claim.getOwnerID()) && (deleteCreativeClaims || !GriefPreventionPlus.getInstance().creativeRulesApply(claim.getWorld()))) {
				// fire event
				final ClaimDeleteEvent event = new ClaimDeleteEvent(claim, sender, Reason.DELETEALL);
				GriefPreventionPlus.getInstance().getServer().getPluginManager().callEvent(event);
				if (!event.isCancelled()) {
					claimsToRemove.add(claim);
				}
			}
		}
		
		for (final Claim claim : claimsToRemove) {
			claim.removeSurfaceFluids(null);
			this.deleteClaim(claim);

			// if in a creative mode world, delete the claim
			if (GriefPreventionPlus.getInstance().creativeRulesApply(claim.getWorld())) {
				GriefPreventionPlus.getInstance().restoreClaim(claim, 0);
			}
		}
	}
	
	/** delete all claims in the specified world.
	 * @return amount of claims deleted */
	public int deleteClaimsInWorld(World world, Player sender) {
		List<Claim> claimsToDelete = new ArrayList<Claim>();
		for (Claim claim : this.claims.values()) {
			if (world.getUID().equals(claim.getWorldUID())) {
				// fire event
				final ClaimDeleteEvent event = new ClaimDeleteEvent(claim, sender, Reason.DELETEALL);
				GriefPreventionPlus.getInstance().getServer().getPluginManager().callEvent(event);
				if (!event.isCancelled()) {
					claimsToDelete.add(claim);
				}
			}
		}
		
		for (final Claim claim : claimsToDelete) {
			claim.removeSurfaceFluids(null);
			this.deleteClaim(claim);

			// if in a creative mode world, delete the claim content
			if (GriefPreventionPlus.getInstance().creativeRulesApply(claim.getWorld())) {
				GriefPreventionPlus.getInstance().restoreClaim(claim, 0);
			}
		}
		
		return claimsToDelete.size();
	}
	
	/** deletes all claims owned by a player on the specified world */
	public void deleteClaimsForPlayer(UUID claimsOwner, Player sender, World world, boolean deleteCreativeClaims) {
		List<Claim> claimsToRemove = new ArrayList<Claim>();
		for (final Claim claim : this.claims.values()) {
			if (world.getUID().equals(claim.getWorldUID()) && (claimsOwner.equals(claim.getOwnerID()) && (deleteCreativeClaims || !GriefPreventionPlus.getInstance().creativeRulesApply(claim.getWorld())))) {
				// fire event
				final ClaimDeleteEvent event = new ClaimDeleteEvent(claim, sender, Reason.DELETEALL);
				GriefPreventionPlus.getInstance().getServer().getPluginManager().callEvent(event);
				if (!event.isCancelled()) {
					claimsToRemove.add(claim);
				}
			}
		}
		
		for (final Claim claim : claimsToRemove) {
			claim.removeSurfaceFluids(null);
			this.deleteClaim(claim);

			// if in a creative mode world, delete the claim
			if (GriefPreventionPlus.getInstance().creativeRulesApply(claim.getWorld())) {
				GriefPreventionPlus.getInstance().restoreClaim(claim, 0);
			}
		}
	}
	
	/** removes a permission node from all claims owned by the specified player */
	public void dropPermissionOnPlayerClaims(UUID ownerId, String permBukkit) {
		final PlayerData ownerData = this.getPlayerData(ownerId);
		if (ownerData != null) {
			this.dbUnsetPerm(ownerId, permBukkit);
			for (final Claim c : ownerData.getClaims()) {
				c.unsetPermission(permBukkit);
			}
		}
	}
	
	/** removes a player trust from all claims owned by the specified player */
	public void dropPermissionOnPlayerClaims(UUID ownerId, UUID playerId) {
		final PlayerData ownerData = this.getPlayerData(ownerId);
		if (ownerData != null) {
			this.dbUnsetPerm(ownerId, playerId);
			for (final Claim c : ownerData.getClaims()) {
				c.unsetPermission(playerId);
			}
		}
	}
	/** get claims map */
	public Map<Integer, Claim> getClaims() {
		return claims;
	}

	/** get a claim by ID */
	public Claim getClaim(int id) {
		return this.claims.get(id);
	}
	
	/** get a claim at specified location, ignoring the height */
	public Claim getClaimAt(Location location) {
		return this.getClaimAt(location, true, null);
	}

	/** get a claim at specified location */
	public Claim getClaimAt(Location location, boolean ignoreHeight) {
		return this.getClaimAt(location, ignoreHeight, null);
	}

	/** get a claim at specified location
	 *  specifying a cached claim will help performances */
	public Claim getClaimAt(Location location, boolean ignoreHeight, Claim cachedClaim) {
		final Claim claim = this.getClaimAt(location, cachedClaim);
		if (ignoreHeight || ((claim != null) && claim.checkHeight(location))) {
			return claim;
		}

		return null;
	}

	/** gets the claim at a specific location
	  cachedClaim can be NULL, but will help performance if you have a
	  reasonable guess about which claim the location is in */
	public Claim getClaimAt(Location location, Claim cachedClaim) {
		// check cachedClaim guess first. if it's in the datastore and the
		// location is inside it, we're done
		if ((cachedClaim != null) && cachedClaim.isInDataStore() && cachedClaim.contains(location, true, false)) {
			return cachedClaim;
		}

		// find a top level claim
		final Claim claim = this.posClaimsGet(location);
		if (claim != null) {
			// when we find a top level claim, if the location is in one of its
			// subdivisions,
			// return the SUBDIVISION, not the top level claim
			for (final Claim subdivision : claim.getChildren()) {
				if (subdivision.contains(location, true, false)) {
					return subdivision;
				}
			}
		}

		return claim;
	}
	
	/** get the message string
	 * @param messageID: a Messages enum value
	 * @param args: preformat the message with the specified strings */
	public String getMessage(Messages messageID, String... args) {
		String message = this.messages[messageID.ordinal()];

		for (int i = 0; i < args.length; i++) {
			final String param = args[i];
			message = message.replace("{" + i + "}", param);
		}

		return message;
	}

	/** gets all the claims 128 blocks range from a location */
	public Map<Integer, Claim> getNearbyClaims(Location location) {
		return this.posClaimsGet(location, 128);
	}

	/** retrieves player data from memory or secondary storage, as necessary
	if the player has never been on the server before, this will return a
	fresh player data with default values */
	public PlayerData getPlayerData(UUID playerID) {
		// first, look in memory
		PlayerData playerData = this.playersData.get(playerID);

		// if not there, build a fresh instance with some blanks for what may be
		// in secondary storage
		if (playerData == null) {
			playerData = new PlayerData(playerID);
			
			// shove that new player data into the hash map cache
			this.playersData.put(playerID, playerData);
		}

		return playerData;
	}

	/**
	 * This method checks if a claim overlaps an existing claim. subdivision are
	 * accepted too
	 *
	 * @return the overlapped claim (or itself if it would overlap a worldguard
	 *         region), null if it doesn't overlap!
	 */
	public Claim overlapsClaims(Claim claim, Claim excludedClaim, Player creatingPlayer) {
		if (claim.getParent() != null) {
			// top claim contains this subclaim
			if (!claim.getParent().contains(claim.getLesserBoundaryCorner(), true, false) || !claim.getParent().contains(claim.getGreaterBoundaryCorner(), true, false)) {
				return claim.getParent();
			}

			// check parent's subclaims
			for (final Claim otherSubclaim : claim.getParent().getChildren()) {
				if ((otherSubclaim == claim) || (otherSubclaim == excludedClaim)) { // exclude this claim
					continue;
				}

				if (otherSubclaim.overlaps(claim)) {
					return otherSubclaim;
				}
			}
		} else {
			// if this claim has subclaims, check that every subclaim is within
			// the top claim
			for (final Claim otherClaim : claim.getChildren()) {
				if (!claim.contains(otherClaim.getGreaterBoundaryCorner(), true, false) || !claim.contains(otherClaim.getLesserBoundaryCorner(), true, false)) {
					return otherClaim;
				}
			}

			// Check for other claims
			for (final Claim otherClaim : this.claims.values()) {
				if ((otherClaim == claim) || (otherClaim == excludedClaim)) {
					continue; // exclude this claim
				}

				if (otherClaim.overlaps(claim)) {
					return otherClaim;
				}
			}

			// if worldguard is installed, also prevent claims from overlapping
			// any worldguard regions
			if (GriefPreventionPlus.getInstance().config.claims_respectWorldGuard && (this.worldGuard != null) && (creatingPlayer != null)) {
				if (!this.worldGuard.canBuild(claim.getWorld(), claim.lesserX, claim.lesserZ, claim.greaterX, claim.greaterZ, creatingPlayer)) {
					return claim;
				}
			}
		}
		return null;
	}

	/** This method will return a set with all claims on the specified range */
	public Map<Integer, Claim> posClaimsGet(Location loc, int blocksRange) {
		int lx = loc.getBlockX() - blocksRange;
		int lz = loc.getBlockZ() - blocksRange;

		int gx = loc.getBlockX() + blocksRange;
		int gz = loc.getBlockZ() + blocksRange;

		final Claim validArea = new Claim(loc.getWorld().getUID(), lx, lz, gx, gz, null, null, null, null, null);

		lx = lx >> 8;
		lz = lz >> 8;
		gx = gx >> 8;
		gz = gz >> 8;

		final Map<Integer, Claim> claims = new HashMap<Integer, Claim>();

		for (int i = lx; i <= gx; i++) {
			for (int j = lz; j <= gz; j++) {
				final Map<Integer, Claim> claimMap = this.posClaims.get(coordsHashCode(i, j));
				if (claimMap != null) {
					for (final Claim claim : claimMap.values()) {
						if (claim.overlaps(validArea)) {
							claims.put(claim.getID(), claim);
						}
					}
				}
			}
		}
		return claims;
	}

	/**
	 * tries to resize a claim see createClaim() for details on return value
	 */
	public ClaimResult resizeClaim(Claim claim, int newx1, int newz1, int newx2, int newz2, Player resizingPlayer) {
		// create a fake claim with new coords
		final Claim newClaim = new Claim(claim.getWorldUID(), newx1, newz1, newx2, newz2, claim.getOwnerID(), null, null, null, claim.id, claim.getCreationDate());
		newClaim.setParent(claim.getParent());
		newClaim.setChildren(claim.getChildren());

		final Claim claimCheck = this.overlapsClaims(newClaim, claim, resizingPlayer);

		if (claimCheck == null) {
			// fire event
			final ClaimResizeEvent event = new ClaimResizeEvent(claim, newClaim, resizingPlayer);
			GriefPreventionPlus.getInstance().getServer().getPluginManager().callEvent(event);
			if (event.isCancelled()) {
				return new ClaimResult(event.getReason());
			}

			// let's update this claim

			this.posClaimsRemove(claim);
			final String oldLoc = claim.locationToString();

			claim.setLocation(claim.getWorldUID(), newx1, newz1, newx2, newz2);
			this.dbUpdateLocation(claim);

			this.posClaimsAdd(claim);

			GriefPreventionPlus.addLogEntry(claim.getOwnerName() + " resized claim id " + claim.id + " from " + oldLoc + " to " + claim.locationToString());
			return new ClaimResult(Result.SUCCESS, claim);
		} else {
			return new ClaimResult(Result.OVERLAP, claimCheck);
		}
	}

	/** saves changes to player data to secondary storage. MUST be called after
	you're done making changes, otherwise a reload will lose them */
	public abstract void savePlayerData(UUID playerID, PlayerData playerData);

	/** saves changes to player data to secondary storage. MUST be called after
	you're done making changes, otherwise a reload will lose them */
	public abstract void savePlayerDataSync(UUID playerID, PlayerData playerData);

	private void addDefault(HashMap<String, CustomizableMessage> defaults, Messages id, String text, String notes) {
		final CustomizableMessage message = new CustomizableMessage(id, text, notes);
		defaults.put(id.name(), message);
	}

	protected void loadMessages() {
		final Messages[] messageIDs = Messages.values();
		this.messages = new String[Messages.values().length];

		final HashMap<String, CustomizableMessage> defaults = new HashMap<String, CustomizableMessage>();

		// initialize defaults
		this.addDefault(defaults, Messages.RespectingClaims, "Now respecting claims.", null);
		this.addDefault(defaults, Messages.IgnoringClaims, "Now ignoring claims.", null);
		this.addDefault(defaults, Messages.NoCreativeUnClaim, "You can't unclaim this land.  You can only make this claim larger or create additional claims.", null);
		this.addDefault(defaults, Messages.SuccessfulAbandon, "Claims abandoned.  You now have {0} available claim blocks.", "0: remaining blocks");
		this.addDefault(defaults, Messages.RestoreNatureActivate, "Ready to restore some nature!  Right click to restore nature, and use /BasicClaims to stop.", null);
		this.addDefault(defaults, Messages.RestoreNatureAggressiveActivate, "Aggressive mode activated.  Do NOT use this underneath anything you want to keep!  Right click to aggressively restore nature, and use /BasicClaims to stop.", null);
		this.addDefault(defaults, Messages.FillModeActive, "Fill mode activated with radius {0}.  Right click an area to fill.", "0: fill radius");
		this.addDefault(defaults, Messages.TransferClaimPermission, "That command requires the administrative claims permission.", null);
		this.addDefault(defaults, Messages.TransferClaimMissing, "There's no claim here.  Stand in the administrative claim you want to transfer.", null);
		this.addDefault(defaults, Messages.TransferClaimAdminOnly, "Only administrative claims may be transferred to a player.", null);
		this.addDefault(defaults, Messages.PlayerNotFound2, "No player by that name has logged in recently.", null);
		this.addDefault(defaults, Messages.TransferTopLevel, "Only top level claims (not subdivisions) may be transferred.  Stand outside of the subdivision and try again.", null);
		this.addDefault(defaults, Messages.TransferSuccess, "Claim transferred.", null);
		this.addDefault(defaults, Messages.TrustListNoClaim, "Stand inside the claim you're curious about.", null);
		this.addDefault(defaults, Messages.ClearPermsOwnerOnly, "Only the claim owner can clear all permissions.", null);
		this.addDefault(defaults, Messages.UntrustIndividualAllClaims, "Revoked {0}'s access to ALL your claims.  To set permissions for a single claim, stand inside it.", "0: untrusted player");
		this.addDefault(defaults, Messages.UntrustEveryoneAllClaims, "Cleared permissions in ALL your claims.  To set permissions for a single claim, stand inside it.", null);
		this.addDefault(defaults, Messages.NoPermissionTrust, "You don't have {0}'s permission to manage permissions here.", "0: claim owner's name");
		this.addDefault(defaults, Messages.ClearPermissionsOneClaim, "Cleared permissions in this claim.  To set permission for ALL your claims, stand outside them.", null);
		this.addDefault(defaults, Messages.UntrustIndividualSingleClaim, "Revoked {0}'s access to this claim.  To set permissions for a ALL your claims, stand outside them.", "0: untrusted player");
		this.addDefault(defaults, Messages.OnlySellBlocks, "Claim blocks may only be sold, not purchased.", null);
		this.addDefault(defaults, Messages.BlockPurchaseCost, "Each claim block costs {0}.  Your balance is {1}.", "0: cost of one block; 1: player's account balance");
		this.addDefault(defaults, Messages.ClaimBlockLimit, "You've reached your claim block limit.  You can't purchase more.", null);
		this.addDefault(defaults, Messages.InsufficientFunds, "You don't have enough money.  You need {0}, but you only have {1}.", "0: total cost; 1: player's account balance");
		this.addDefault(defaults, Messages.PurchaseConfirmation, "Withdrew {0} from your account.  You now have {1} available claim blocks.", "0: total cost; 1: remaining blocks");
		this.addDefault(defaults, Messages.OnlyPurchaseBlocks, "Claim blocks may only be purchased, not sold.", null);
		this.addDefault(defaults, Messages.BlockSaleValue, "Each claim block is worth {0}.  You have {1} available for sale.", "0: block value; 1: available blocks");
		this.addDefault(defaults, Messages.NotEnoughBlocksForSale, "You don't have that many claim blocks available for sale.", null);
		this.addDefault(defaults, Messages.BlockSaleConfirmation, "Deposited {0} in your account.  You now have {1} available claim blocks.", "0: amount deposited; 1: remaining blocks");
		this.addDefault(defaults, Messages.AdminClaimsMode, "Administrative claims mode active.  Any claims created will be free and editable by other administrators.", null);
		this.addDefault(defaults, Messages.BasicClaimsMode, "Returned to basic claim creation mode.", null);
		this.addDefault(defaults, Messages.SubdivisionMode, "Subdivision mode.  Use your shovel to create subdivisions in your existing claims.  Use /basicclaims to exit.", null);
		this.addDefault(defaults, Messages.SubdivisionVideo2, "Click for Subdivision Help: {0}", "0:video URL");
		this.addDefault(defaults, Messages.DeleteClaimMissing, "There's no claim here.", null);
		this.addDefault(defaults, Messages.DeletionSubdivisionWarning, "This claim includes subdivisions.  If you're sure you want to delete it, use /DeleteClaim again.", null);
		this.addDefault(defaults, Messages.DeleteSuccess, "Claim deleted.", null);
		this.addDefault(defaults, Messages.CantDeleteAdminClaim, "You don't have permission to delete administrative claims.", null);
		this.addDefault(defaults, Messages.DeleteAllSuccess, "Deleted all of {0}'s claims.", "0: owner's name");
		this.addDefault(defaults, Messages.NoDeletePermission, "You don't have permission to delete claims.", null);
		this.addDefault(defaults, Messages.AllAdminDeleted, "Deleted all administrative claims.", null);
		this.addDefault(defaults, Messages.AdjustBlocksSuccess, "Adjusted {0}'s bonus claim blocks by {1}.  New total bonus blocks: {2}.", "0: player; 1: adjustment; 2: new total");
		this.addDefault(defaults, Messages.AdjustBlocksAllSuccess, "Adjusted all online players' bonus claim blocks by {0}.", "0: adjustment amount");
		this.addDefault(defaults, Messages.NotTrappedHere, "You can build here.  Save yourself.", null);
		this.addDefault(defaults, Messages.RescuePending, "If you stay put for 10 seconds, you'll be teleported out.  Please wait.", null);
		this.addDefault(defaults, Messages.AbandonClaimMissing, "Stand in the claim you want to delete, or consider /AbandonAllClaims.", null);
		this.addDefault(defaults, Messages.NotYourClaim, "This isn't your claim.", null);
		this.addDefault(defaults, Messages.DeleteTopLevelClaim, "To delete a subdivision, stand inside it.  Otherwise, use /AbandonTopLevelClaim to delete this claim and all subdivisions.", null);
		this.addDefault(defaults, Messages.AbandonSuccess, "Claim abandoned.  You now have {0} available claim blocks.", "0: remaining claim blocks");
		this.addDefault(defaults, Messages.CantGrantThatPermission, "You can't grant a permission you don't have yourself.", null);
		this.addDefault(defaults, Messages.GrantPermissionNoClaim, "Stand inside the claim where you want to grant permission.", null);
		this.addDefault(defaults, Messages.GrantPermissionConfirmation, "Granted {0} permission to {1} {2}.", "0: target player; 1: permission description; 2: scope (changed claims)");
		this.addDefault(defaults, Messages.ManageUniversalPermissionsInstruction, "To manage permissions for ALL your claims, stand outside them.", null);
		this.addDefault(defaults, Messages.ManageOneClaimPermissionsInstruction, "To manage permissions for a specific claim, stand inside it.", null);
		this.addDefault(defaults, Messages.CollectivePublic, "the public", "as in 'granted the public permission to...'");
		this.addDefault(defaults, Messages.BuildPermission, "build", null);
		this.addDefault(defaults, Messages.ContainersPermission, "access containers and animals", null);
		this.addDefault(defaults, Messages.EntryPermission, "enter", null);
		this.addDefault(defaults, Messages.AccessPermission, "use buttons and levers", null);
		this.addDefault(defaults, Messages.PermissionsPermission, "manage permissions", null);
		this.addDefault(defaults, Messages.LocationCurrentClaim, "in this claim", null);
		this.addDefault(defaults, Messages.LocationAllClaims, "in all your claims", null);
		this.addDefault(defaults, Messages.PvPImmunityStart, "You're protected from attack by other players as long as your inventory is empty.", null);
		this.addDefault(defaults, Messages.DonateItemsInstruction, "To give away the item(s) in your hand, left-click the chest again.", null);
		this.addDefault(defaults, Messages.ChestFull, "This chest is full.", null);
		this.addDefault(defaults, Messages.DonationSuccess, "Item(s) transferred to chest!", null);
		this.addDefault(defaults, Messages.PlayerTooCloseForFire, "You can't start a fire this close to {0}.", "0: other player's name");
		this.addDefault(defaults, Messages.TooDeepToClaim, "This chest can't be protected because it's too deep underground.  Consider moving it.", null);
		this.addDefault(defaults, Messages.ChestClaimConfirmation, "This chest is protected.", null);
		this.addDefault(defaults, Messages.AutomaticClaimNotification, "This chest and nearby blocks are protected from breakage and theft.", null);
		this.addDefault(defaults, Messages.UnprotectedChestWarning, "This chest is NOT protected.  Consider using a golden shovel to expand an existing claim or to create a new one.", null);
		this.addDefault(defaults, Messages.ThatPlayerPvPImmune, "You can't injure defenseless players.", null);
		this.addDefault(defaults, Messages.CantFightWhileImmune, "You can't fight someone while you're protected from PvP.", null);
		this.addDefault(defaults, Messages.NoDamageClaimedEntity, "That belongs to {0}.", "0: owner name");
		this.addDefault(defaults, Messages.ShovelBasicClaimMode, "Shovel returned to basic claims mode.", null);
		this.addDefault(defaults, Messages.RemainingBlocks, "You may claim up to {0} more blocks.", "0: remaining blocks");
		this.addDefault(defaults, Messages.CreativeBasicsVideo2, "Click for Land Claim Help: {0}", "{0}: video URL");
		this.addDefault(defaults, Messages.SurvivalBasicsVideo2, "Click for Land Claim Help: {0}", "{0}: video URL");
		this.addDefault(defaults, Messages.TrappedChatKeyword, "trapped", "When mentioned in chat, players get information about the /trapped command.");
		this.addDefault(defaults, Messages.TrappedInstructions, "Are you trapped in someone's land claim?  Try the /trapped command.", null);
		this.addDefault(defaults, Messages.PvPNoDrop, "You can't drop items while in PvP combat.", null);
		this.addDefault(defaults, Messages.PvPNoContainers, "You can't access containers during PvP combat.", null);
		this.addDefault(defaults, Messages.PvPImmunityEnd, "Now you can fight with other players.", null);
		this.addDefault(defaults, Messages.NoBedPermission, "{0} hasn't given you permission to sleep here.", "0: claim owner");
		this.addDefault(defaults, Messages.NoWildernessBuckets, "You may only dump buckets inside your claim(s) or underground.", null);
		this.addDefault(defaults, Messages.NoLavaNearOtherPlayer, "You can't place lava this close to {0}.", "0: nearby player");
		this.addDefault(defaults, Messages.TooFarAway, "That's too far away.", null);
		this.addDefault(defaults, Messages.BlockNotClaimed, "No one has claimed this block.", null);
		this.addDefault(defaults, Messages.BlockClaimed, "That block has been claimed by {0}.", "0: claim owner");
		this.addDefault(defaults, Messages.RestoreNaturePlayerInChunk, "Unable to restore.  {0} is in that chunk.", "0: nearby player");
		this.addDefault(defaults, Messages.NoCreateClaimPermission, "You don't have permission to claim land.", null);
		this.addDefault(defaults, Messages.ResizeClaimTooSmall, "This new size would be too small.  Claims must be at least {0} x {0}.", "0: minimum claim size");
		this.addDefault(defaults, Messages.ResizeNeedMoreBlocks, "You don't have enough blocks for this size.  You need {0} more.", "0: how many needed");
		this.addDefault(defaults, Messages.ClaimResizeSuccess, "Claim resized.  {0} available claim blocks remaining.", "0: remaining blocks");
		this.addDefault(defaults, Messages.ResizeFailOverlap, "Can't resize here because it would overlap another nearby claim.", null);
		this.addDefault(defaults, Messages.ResizeStart, "Resizing claim.  Use your shovel again at the new location for this corner.", null);
		this.addDefault(defaults, Messages.ResizeFailOverlapSubdivision, "You can't create a subdivision here because it would overlap another subdivision.  Consider /abandonclaim to delete it, or use your shovel at a corner to resize it.", null);
		this.addDefault(defaults, Messages.SubdivisionStart, "Subdivision corner set!  Use your shovel at the location for the opposite corner of this new subdivision.", null);
		this.addDefault(defaults, Messages.CreateSubdivisionOverlap, "Your selected area overlaps another subdivision.", null);
		this.addDefault(defaults, Messages.SubdivisionSuccess, "Subdivision created!  Use /trust to share it with friends.", null);
		this.addDefault(defaults, Messages.CreateClaimFailOverlap, "You can't create a claim here because it would overlap your other claim.  Use /abandonclaim to delete it, or use your shovel at a corner to resize it.", null);
		this.addDefault(defaults, Messages.CreateClaimFailOverlapOtherPlayer, "You can't create a claim here because it would overlap {0}'s claim.", "0: other claim owner");
		this.addDefault(defaults, Messages.ClaimsDisabledWorld, "Land claims are disabled in this world.", null);
		this.addDefault(defaults, Messages.ClaimStart, "Claim corner set!  Use the shovel again at the opposite corner to claim a rectangle of land.  To cancel, put your shovel away.", null);
		this.addDefault(defaults, Messages.NewClaimTooSmall, "This claim would be too small.  Any claim must be at least {0} x {0}.", "0: minimum claim size");
		this.addDefault(defaults, Messages.CreateClaimInsufficientBlocks, "You don't have enough blocks to claim that entire area.  You need {0} more blocks.", "0: additional blocks needed");
		this.addDefault(defaults, Messages.AbandonClaimAdvertisement, "To delete another claim and free up some blocks, use /AbandonClaim.", null);
		this.addDefault(defaults, Messages.CreateClaimFailOverlapShort, "Your selected area overlaps an existing claim.", null);
		this.addDefault(defaults, Messages.CreateClaimSuccess, "Claim created!  Use /trust to share it with friends.", null);
		this.addDefault(defaults, Messages.RescueAbortedMoved, "You moved!  Rescue cancelled.", null);
		this.addDefault(defaults, Messages.OnlyOwnersModifyClaims, "Only {0} can modify this claim.", "0: owner name");
		this.addDefault(defaults, Messages.NoBuildPvP, "You can't build in claims during PvP combat.", null);
		this.addDefault(defaults, Messages.NoBuildPermission, "You don't have {0}'s permission to build here.", "0: owner name");
		this.addDefault(defaults, Messages.NoAccessPermission, "You don't have {0}'s permission to use that.", "0: owner name.  access permission controls buttons, levers, and beds");
		this.addDefault(defaults, Messages.NoEntryPermission, "You don't have {0}'s permission to enter this claim.", "0: owner name");
		this.addDefault(defaults, Messages.NoContainersPermission, "You don't have {0}'s permission to use that.", "0: owner's name.  containers also include crafting blocks");
		this.addDefault(defaults, Messages.OwnerNameForAdminClaims, "an administrator", "as in 'You don't have an administrator's permission to build here.'");
		this.addDefault(defaults, Messages.ClaimTooSmallForEntities, "This claim isn't big enough for that.  Try enlarging it.", null);
		this.addDefault(defaults, Messages.TooManyEntitiesInClaim, "This claim has too many entities already.  Try enlarging the claim or removing some animals, monsters, paintings, or minecarts.", null);
		this.addDefault(defaults, Messages.YouHaveNoClaims, "You don't have any land claims.", null);
		this.addDefault(defaults, Messages.ConfirmFluidRemoval, "Abandoning this claim will remove lava inside the claim.  If you're sure, use /AbandonClaim again.", null);
		this.addDefault(defaults, Messages.AutoBanNotify, "Auto-banned {0}({1}).  See logs for details.", null);
		this.addDefault(defaults, Messages.AdjustGroupBlocksSuccess, "Adjusted bonus claim blocks for players with the {0} permission by {1}.  New total: {2}.", "0: permission; 1: adjustment amount; 2: new total bonus");
		this.addDefault(defaults, Messages.InvalidPermissionID, "Please specify a player name, or a permission in [brackets].", null);
		this.addDefault(defaults, Messages.UntrustOwnerOnly, "Only {0} can revoke permissions here.", "0: claim owner's name");
		this.addDefault(defaults, Messages.HowToClaimRegex, "(^|.*\\W)how\\W.*\\W(claim|protect|lock)(\\W.*|$)", "This is a Java Regular Expression.  Look it up before editing!  It's used to tell players about the demo video when they ask how to claim land.");
		this.addDefault(defaults, Messages.NoBuildOutsideClaims, "You can't build here unless you claim some land first.", null);
		this.addDefault(defaults, Messages.PlayerOfflineTime, "  Last login: {0} days ago.", "0: number of full days since last login");
		this.addDefault(defaults, Messages.BuildingOutsideClaims, "Other players can build here, too.  Consider creating a land claim to protect your work!", null);
		this.addDefault(defaults, Messages.TrappedWontWorkHere, "Sorry, unable to find a safe location to teleport you to.  Contact an admin, or consider /kill if you don't want to wait.", null);
		this.addDefault(defaults, Messages.CommandBannedInPvP, "You can't use that command while in PvP combat.", null);
		this.addDefault(defaults, Messages.UnclaimCleanupWarning, "The land you've unclaimed may be changed by other players or cleaned up by administrators.  If you've built something there you want to keep, you should reclaim it.", null);
		this.addDefault(defaults, Messages.BuySellNotConfigured, "Sorry, buying anhd selling claim blocks is disabled.", null);
		this.addDefault(defaults, Messages.NoTeleportPvPCombat, "You can't teleport while fighting another player.", null);
		this.addDefault(defaults, Messages.NoTNTDamageAboveSeaLevel, "Warning: TNT will not destroy blocks above sea level.", null);
		this.addDefault(defaults, Messages.NoTNTDamageClaims, "Warning: TNT will not destroy claimed blocks.", null);
		this.addDefault(defaults, Messages.IgnoreClaimsAdvertisement, "To override, use /IgnoreClaims.", null);
		this.addDefault(defaults, Messages.NoPermissionForCommand, "You don't have permission to do that.", null);
		this.addDefault(defaults, Messages.ClaimsListNoPermission, "You don't have permission to get information about another player's land claims.", null);
		this.addDefault(defaults, Messages.ExplosivesDisabled, "This claim is now protected from explosions.  Use /ClaimExplosions again to disable.", null);
		this.addDefault(defaults, Messages.ExplosivesEnabled, "This claim is now vulnerable to explosions.  Use /ClaimExplosions again to re-enable protections.", null);
		this.addDefault(defaults, Messages.ClaimExplosivesAdvertisement, "To allow explosives to destroy blocks in this land claim, use /ClaimExplosions.", null);
		this.addDefault(defaults, Messages.PlayerInPvPSafeZone, "That player is in a PvP safe zone.", null);
		this.addDefault(defaults, Messages.NoPistonsOutsideClaims, "Warning: Pistons won't move blocks outside land claims.", null);
		this.addDefault(defaults, Messages.SoftMuted, "Soft-muted {0}.", "0: The changed player's name.");
		this.addDefault(defaults, Messages.UnSoftMuted, "Un-soft-muted {0}.", "0: The changed player's name.");
		this.addDefault(defaults, Messages.DropUnlockAdvertisement, "Other players can't pick up your dropped items unless you /UnlockDrops first.", null);
		this.addDefault(defaults, Messages.PickupBlockedExplanation, "You can't pick this up unless {0} uses /UnlockDrops.", "0: The item stack's owner.");
		this.addDefault(defaults, Messages.DropUnlockConfirmation, "Unlocked your drops.  Other players may now pick them up (until you die again).", null);
		this.addDefault(defaults, Messages.AdvertiseACandACB, "You may use /ACB to give yourself more claim blocks, or /AdminClaims to create a free administrative claim.", null);
		this.addDefault(defaults, Messages.AdvertiseAdminClaims, "You could create an administrative land claim instead using /AdminClaims, which you'd share with other administrators.", null);
		this.addDefault(defaults, Messages.AdvertiseACB, "You may use /ACB to give yourself more claim blocks.", null);
		this.addDefault(defaults, Messages.NotYourPet, "That belongs to {0} until it's given to you with /GivePet.", "0: owner name");
		this.addDefault(defaults, Messages.PetGiveawayConfirmation, "Pet transferred.", null);
		this.addDefault(defaults, Messages.PetTransferCancellation, "Pet giveaway cancelled.", null);
		this.addDefault(defaults, Messages.ReadyToTransferPet, "Ready to transfer!  Right-click the pet you'd like to give away, or cancel with /GivePet cancel.", null);
		this.addDefault(defaults, Messages.AvoidGriefClaimLand, "Prevent grief!  If you claim your land, you will be grief-proof.", null);
		this.addDefault(defaults, Messages.BecomeMayor, "Subdivide your land claim and become a mayor!", null);
		this.addDefault(defaults, Messages.ClaimCreationFailedOverClaimCountLimit, "You've reached your limit on land claims.  Use /AbandonClaim to remove one before creating another.", null);
		this.addDefault(defaults, Messages.CreateClaimFailOverlapRegion, "You can't claim all of this because you're not allowed to build here.", null);
		this.addDefault(defaults, Messages.ResizeFailOverlapRegion, "You don't have permission to build there, so you can't claim that area.", null);
		this.addDefault(defaults, Messages.NoBuildPortalPermission, "You can't use this portal because you don't have {0}'s permission to build an exit portal in the destination land claim.", "0: Destination land claim owner's name.");
		this.addDefault(defaults, Messages.ShowNearbyClaims, "Found {0} land claims.", "0: Number of claims found.");
		this.addDefault(defaults, Messages.NoChatUntilMove, "Sorry, but you have to move a little more before you can chat.  We get lots of spam bots here.  :)", null);
		this.addDefault(defaults, Messages.SetClaimBlocksSuccess, "Updated accrued claim blocks.", null);

		// load the config file
		final FileConfiguration config = YamlConfiguration.loadConfiguration(new File(messagesFilePath));

		// for each message ID
		for (int i = 0; i < messageIDs.length; i++) {
			// get default for this message
			final Messages messageID = messageIDs[i];
			CustomizableMessage messageData = defaults.get(messageID.name());

			// if default is missing, log an error and use some fake data for
			// now so that the plugin can run
			if (messageData == null) {
				GriefPreventionPlus.addLogEntry("Missing message for " + messageID.name() + ".  Please contact the developer.");
				messageData = new CustomizableMessage(messageID, "Missing message!  ID: " + messageID.name() + ".  Please contact a server admin.", null);
			}

			// read the message from the file, use default if necessary
			this.messages[messageID.ordinal()] = config.getString("Messages." + messageID.name() + ".Text", messageData.getText());
			config.set("Messages." + messageID.name() + ".Text", this.messages[messageID.ordinal()]);

			if (messageData.getNotes() != null) {
				messageData.setNotes(config.getString("Messages." + messageID.name() + ".Notes", messageData.getNotes()));
				config.set("Messages." + messageID.name() + ".Notes", messageData.getNotes());
			}
		}

		// save any changes
		try {
			config.save(DataStore.messagesFilePath);
		} catch (final IOException exception) {
			GriefPreventionPlus.addLogEntry("Unable to write to the configuration file at \"" + DataStore.messagesFilePath + "\"");
		}

		defaults.clear();
		System.gc();
	}
	
	// adds a claim to the datastore, making it an effective claim
	void addClaim(Claim newClaim, boolean writeToStorage) {
		// subdivisions are easy
		if (newClaim.getParent() != null) {
			newClaim.getParent().getChildren().add(newClaim);
			if (writeToStorage) {
				this.dbNewClaim(newClaim);
			}
			return;
		}

		if (writeToStorage) { // write the new claim on the db, so we get the id
			// generated by the database
			this.dbNewClaim(newClaim);
			GriefPreventionPlus.addLogEntry(newClaim.getOwnerName() + " made a new claim (id " + newClaim.id + ") at " + newClaim.locationToString());
		}

		this.claims.put(newClaim.id, newClaim);

		this.posClaimsAdd(newClaim);

		// except for administrative claims (which have no owner), update the
		// owner's playerData with the new claim
		if (!newClaim.isAdminClaim() && writeToStorage) {
			final PlayerData ownerData = this.getPlayerData(newClaim.getOwnerID());
			ownerData.getClaims().add(newClaim);
			this.savePlayerData(newClaim.getOwnerID(), ownerData);
		}
	}

	abstract int clearOrphanClaims();

	abstract void close();

	abstract void dbNewClaim(Claim claim);

	abstract void dbSetPerm(Integer claimId, String permString, int perm);

	abstract void dbSetPerm(Integer claimId, UUID playerId, int perm);

	/** Unset all claim's perms */
	abstract void dbUnsetPerm(Integer claimId);

	/** Unset permBukkit's perm from claim */
	abstract void dbUnsetPerm(Integer claimId, String permString);

	/** Unset playerId's perm from claim */
	abstract void dbUnsetPerm(Integer claimId, UUID playerId);

	/** Unset all player claims' perms */
	abstract void dbUnsetPerm(UUID playerId);

	/** Unset permbukkit perms from all owner's claim */
	abstract void dbUnsetPerm(UUID owner, String permString);

	/** Unset playerId perms from all owner's claim */
	abstract void dbUnsetPerm(UUID owner, UUID playerId);

	abstract void dbUpdateLocation(Claim claim);

	abstract void dbUpdateOwner(Claim claim);

	// deletes a claim from the database (this delete subclaims too)
	abstract void deleteClaimFromSecondaryStorage(Claim claim);

	// gets the number of bonus blocks a player has from his permissions
	// Bukkit doesn't allow for checking permissions of an offline player.
	// this will return 0 when he's offline, and the correct number when online.
	int getGroupBonusBlocks(UUID playerID) {
		final Player player = GriefPreventionPlus.getInstance().getServer().getPlayer(playerID);
		if (player != null) {
			int bonusBlocks = 0;
			for (final Entry<String, Integer> e : this.permissionToBonusBlocksMap.entrySet()) {
				if ((player != null) && player.hasPermission(e.getKey())) {
					bonusBlocks += e.getValue();
				}
			}
			return bonusBlocks;
		} else {
			return 0;
		}
	}

	abstract PlayerData getPlayerDataFromStorage(UUID playerID);
	
	abstract void cachePlayersData();
	
	void posClaimsAdd(Claim claim) {
		final int lx = claim.getLesserBoundaryCorner().getBlockX() >> 8;
		final int lz = claim.getLesserBoundaryCorner().getBlockZ() >> 8;

		final int gx = claim.getGreaterBoundaryCorner().getBlockX() >> 8;
		final int gz = claim.getGreaterBoundaryCorner().getBlockZ() >> 8;

		for (int i = lx; i <= gx; i++) {
			for (int j = lz; j <= gz; j++) {
				Map<Integer, Claim> claimMap = this.posClaims.get(coordsHashCode(i, j));
				if (claimMap == null) {
					claimMap = new HashMap<Integer, Claim>();
					this.posClaims.put(coordsHashCode(i, j), claimMap);
				}
				claimMap.put(claim.getID(), claim);
			}
		}
	}

	Claim posClaimsGet(Location loc) {
		final Map<Integer, Claim> claimMap = this.posClaims.get(coordsHashCode(loc.getBlockX() >> 8, loc.getBlockZ() >> 8));
		if (claimMap != null) {
			for (final Claim claim : claimMap.values()) {
				if (claim.contains(loc, true, false)) {
					return claim;
				}
			}
		}
		return null;
	}

	void posClaimsRemove(Claim claim) {
		final int lx = claim.getLesserBoundaryCorner().getBlockX() >> 8;
		final int lz = claim.getLesserBoundaryCorner().getBlockZ() >> 8;

		final int gx = claim.getGreaterBoundaryCorner().getBlockX() >> 8;
		final int gz = claim.getGreaterBoundaryCorner().getBlockZ() >> 8;

		for (int i = lx; i <= gx; i++) {
			for (int j = lz; j <= gz; j++) {
				final Map<Integer, Claim> claimMap = this.posClaims.get(coordsHashCode(i, j));
				if (claimMap != null) {
					claimMap.remove(claim.getID());
				}
			}
		}
	}

	// updates the database with a group's bonus blocks
	abstract void saveGroupBonusBlocks(String groupName, int currentValue);

	// path information, for where stuff stored on disk is well... stored
	protected final static String dataLayerFolderPath = "plugins" + File.separator + "GriefPreventionData";

	final static String configFilePath = dataLayerFolderPath + File.separator + "config.yml";

	final static String messagesFilePath = dataLayerFolderPath + File.separator + "messages.yml";

	// video links
	static final String SURVIVAL_VIDEO_URL = "" + ChatColor.DARK_AQUA + ChatColor.UNDERLINE + "bit.ly/mcgpuser";

	static final String CREATIVE_VIDEO_URL = "" + ChatColor.DARK_AQUA + ChatColor.UNDERLINE + "bit.ly/mcgpcrea";

	static final String SUBDIVISION_VIDEO_URL = "" + ChatColor.DARK_AQUA + ChatColor.UNDERLINE + "bit.ly/mcgpsub";
	
	/** Converts an array of 16 bytes to an UUID */
	public static UUID toUUID(byte[] bytes) {
		if (bytes.length != 16) {
			throw new IllegalArgumentException();
		}
		int i = 0;
		long msl = 0;
		for (; i < 8; i++) {
			msl = (msl << 8) | (bytes[i] & 0xFF);
		}
		long lsl = 0;
		for (; i < 16; i++) {
			lsl = (lsl << 8) | (bytes[i] & 0xFF);
		}
		return new UUID(msl, lsl);
	}

	/** Converts an UUID to an hex number using the 0x format */
	public static String UUIDtoHexString(UUID uuid) {
		if (uuid == null) {
			return "0";
		}
		return "0x" + org.apache.commons.lang.StringUtils.leftPad(Long.toHexString(uuid.getMostSignificantBits()), 16, "0") + org.apache.commons.lang.StringUtils.leftPad(Long.toHexString(uuid.getLeastSignificantBits()), 16, "0");
	}

	public static int coordsHashCode(int x, int z) {
		return (z ^ (x << 16));
	}
}
