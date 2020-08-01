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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.OfflinePlayer;
import org.bukkit.World;
import org.bukkit.World.Environment;
import org.bukkit.block.Block;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;

import net.kaikk.mc.gpp.ClaimResult.Result;
import net.kaikk.mc.gpp.events.ClaimDeleteEvent;
import net.kaikk.mc.gpp.events.ClaimDeleteEvent.Reason;
import net.kaikk.mc.gpp.events.ClaimOwnerTransfer;
import net.kaikk.mc.gpp.visualization.Visualization;
import net.kaikk.mc.gpp.visualization.VisualizationType;

@SuppressWarnings("deprecation")
public class CommandExec implements CommandExecutor {
	// handles slash commands
	GriefPreventionPlus gpp = GriefPreventionPlus.getInstance();
	DataStore dataStore = this.gpp.getDataStore();

	@Override
	public boolean onCommand(CommandSender sender, Command cmd, String commandLabel, String[] args) {

		Player player = null;
		if (sender instanceof Player) {
			player = (Player) sender;
		}

		// Commands added on GPP
		// claim
		if (cmd.getName().equalsIgnoreCase("claim") && (player != null)) {
			if (args.length != 1) {
				return false;
			}
			try {
				if (!GriefPreventionPlus.getInstance().claimsEnabledForWorld(player.getWorld())) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.ClaimsDisabledWorld);
					return true;
				}
				
				final int range = Integer.valueOf(args[0]);
				final int side = (range * 2) + 1;
				if (side < GriefPreventionPlus.getInstance().config.claims_minSize) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NewClaimTooSmall, String.valueOf(GriefPreventionPlus.getInstance().config.claims_minSize));
					return true;
				}
				
				final int newClaimArea = side * side;
				final PlayerData playerData = this.dataStore.getPlayerData(player.getUniqueId());
				if ((GriefPreventionPlus.getInstance().config.claims_maxClaimsPerPlayer > 0) && !player.hasPermission("griefprevention.overrideclaimcountlimit") && (playerData.getClaims().size() >= GriefPreventionPlus.getInstance().config.claims_maxClaimsPerPlayer)) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.ClaimCreationFailedOverClaimCountLimit);
					return true;
				}
				
				
				final int remainingBlocks = playerData.getRemainingClaimBlocks();
				if (newClaimArea > remainingBlocks) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.CreateClaimInsufficientBlocks, String.valueOf(newClaimArea - remainingBlocks));
					PlayerEventHandler.tryAdvertiseAdminAlternatives(player);
					return true;
				}

				this.createClaim(player, playerData, range);
			} catch (final NumberFormatException e) {
				return false;
			}

			return true;
		}
		
		if (cmd.getName().equalsIgnoreCase("adminclaim") && (player != null)) {
			if (args.length != 1) {
				return false;
			}
			
			if (!player.hasPermission("griefprevention.adminclaims")) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.TransferClaimPermission);
				return true;
			}
			try {
				final int range = Integer.valueOf(args[0]);
				if (range < 1) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NewClaimTooSmall, String.valueOf(GriefPreventionPlus.getInstance().config.claims_minSize));
					return true;
				}
				PlayerData playerData = this.dataStore.getPlayerData(player.getUniqueId());
				playerData.shovelMode = ShovelMode.Admin;
				this.createClaim(player, playerData, range);
				return true;
			} catch (final NumberFormatException e) {
				return false;
			}
		}

		if (cmd.getName().equalsIgnoreCase("clearorphanclaims")) {
			if (player != null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Success, "Removed " + GriefPreventionPlus.getInstance().getDataStore().clearOrphanClaims() + " orphan claims.");
				GriefPreventionPlus.addLogEntry(player.getName() + " cleared orphan claims.");
			} else {
				sender.sendMessage("Removed " + GriefPreventionPlus.getInstance().getDataStore().clearOrphanClaims() + " orphan claims.");
			}

			return true;
		}
		
		if (cmd.getName().equalsIgnoreCase("tpclaim")) {
			if (player == null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, "Console can't run this command!");
				return true;
			}
			
			if (args.length<1) {
				return false;
			}
			
			if (args.length>1 && !player.hasPermission("griefprevention.tpclaim.others")) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NoPermissionForCommand);
				return false;
			}
			
			try {
				Claim claim = GriefPreventionPlus.getInstance().getDataStore().getClaim(Integer.valueOf(args[0]));
				if (claim==null) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, "Claim not found");
					return false;
				}
				
				Location loc = claim.getLesserBoundaryCorner();
				loc = loc.getWorld().getHighestBlockAt(loc).getLocation();
				Player otherPlayer;
				if (args.length==1) {
					otherPlayer = player;
				} else {
					otherPlayer = Bukkit.getPlayer(args[1]);
					if (otherPlayer == null) {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, "Player not found");
						return false;
					}
				}
				otherPlayer.teleport(loc);
				if (player != otherPlayer) {
					GriefPreventionPlus.sendMessage(otherPlayer, TextMode.Info, "Teleported "+otherPlayer.getName()+" to claim ("+claim.getID()+") at "+"[" + loc.getWorld().getName() + ", " + loc.getBlockX() + "," + loc.getBlockY() + "," + loc.getBlockZ() + "]");
				}
				GriefPreventionPlus.sendMessage(otherPlayer, TextMode.Info, "Teleported to claim ("+claim.getID()+") at "+"[" + loc.getWorld().getName() + ", " + loc.getBlockX() + "," + loc.getBlockY() + "," + loc.getBlockZ() + "]");
				return true;
			} catch (NumberFormatException e) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, "Invalid id");
				return false;
			}
		}
		
		if (cmd.getName().equalsIgnoreCase("amountofclaims")) {
			if (args.length==0) {
				GriefPreventionPlus.sendMessage(sender, TextMode.Info, "Total amount of claims: " + gpp.getDataStore().getClaims().size());
			} else {
				World w = Bukkit.getWorld(args[0]);
				if (w == null) {
					GriefPreventionPlus.sendMessage(sender, TextMode.Err, "Invalid world name");
					return false;
				}
				
				UUID uuid = w.getUID();
				
				int n = 0;
				for (Claim c : gpp.getDataStore().getClaims().values()) {
					if (c.getWorldUID().equals(uuid)) {
						n++;
					}
				}
				
				GriefPreventionPlus.sendMessage(sender, TextMode.Info, "Total amount of claims on "+w.getName()+": " + n);
			}
			return true;
		}
		
		if (cmd.getName().equalsIgnoreCase("deleteplayerclaims")) {
			if (args.length==0) {
				return false;
			}
			OfflinePlayer oPlayer = Bukkit.getOfflinePlayer(args[0]);
			if (oPlayer.getLastPlayed()==0 && !oPlayer.isOnline()) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, "The player has never played on this server.");
				return true;
			}
			
			if (args.length>1) {
				World world = Bukkit.getWorld(args[1]);
				if (world == null) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, "The specified world doesn't exist.");
					return false;
				}
				
				this.gpp.getDataStore().deleteClaimsForPlayer(oPlayer.getUniqueId(), player, world, true);
				GriefPreventionPlus.sendMessage(player, TextMode.Info, "Deleted "+oPlayer.getName()+" claims in world \""+world.getName()+"\".");
			} else {
				this.gpp.getDataStore().deleteClaimsForPlayer(oPlayer.getUniqueId(), player, true);
				GriefPreventionPlus.sendMessage(player, TextMode.Info, "Deleted "+oPlayer.getName()+" claims.");
			}
			
			return true;
		}
		
		if (cmd.getName().equalsIgnoreCase("deleteclaimsinworld")) {
			if (args.length==0) {
				return false;
			}
			
			World world = Bukkit.getWorld(args[0]);
			if (world==null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, "The specified world doesn't exist.");
				return true;
			}
			
			GriefPreventionPlus.sendMessage(player, TextMode.Info, "Deleted "+this.gpp.getDataStore().deleteClaimsInWorld(world, player)+" claims in world \""+world.getName()+"\".");
			return true;
		}
		
		if (cmd.getName().equalsIgnoreCase("autotrust") && (player != null)) {
			final Claim claim = this.dataStore.getClaimAt(player.getLocation(), true);
			if (claim==null) {
				return false;
			}
			
			if (claim.canGrantPermission(player) != null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NoPermissionTrust, claim.getOwnerName());
				return true;
			}
			
			claim.autoTrust = System.currentTimeMillis()+30000;
			player.sendMessage(ChatColor.GREEN+"The next player that breaks or places a block in the next 30 seconds on this claim will get automatically trusted."); // TODO move to messages
			return true;
		}

		// GP's commands
		// abandonclaim
		if (cmd.getName().equalsIgnoreCase("abandonclaim") && (player != null)) {
			try {
				return this.abandonClaimHandler(player, false, (args.length>0 ? Integer.valueOf(args[0]) : -1));
			} catch (NumberFormatException e) {
				return false;
			}
		}

		// abandontoplevelclaim
		if (cmd.getName().equalsIgnoreCase("abandontoplevelclaim") && (player != null)) {
			return this.abandonClaimHandler(player, true);
		}

		// ignoreclaims
		if (cmd.getName().equalsIgnoreCase("ignoreclaims") && (player != null)) {
			final PlayerData playerData = this.gpp.getDataStore().getPlayerData(player.getUniqueId());

			playerData.ignoreClaims = !playerData.ignoreClaims;

			// toggle ignore claims mode on or off
			if (!playerData.ignoreClaims) {
				GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.RespectingClaims);
			} else {
				GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.IgnoringClaims);
			}

			return true;
		}

		// abandonallclaims
		else if (cmd.getName().equalsIgnoreCase("abandonallclaims") && (player != null)) {
			if (args.length != 0) {
				return false;
			}

			// count claims
			final PlayerData playerData = this.gpp.getDataStore().getPlayerData(player.getUniqueId());
			final int originalClaimCount = playerData.getClaims().size();

			// check count
			if (originalClaimCount == 0) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.YouHaveNoClaims);
				return true;
			}

			// adjust claim blocks
			for (final Claim claim : playerData.getClaims()) {
				playerData.setAccruedClaimBlocks(playerData.getAccruedClaimBlocks() - (int) Math.ceil((claim.getArea() * (1 - this.gpp.config.claims_abandonReturnRatio))));
			}

			// delete them
			this.gpp.getDataStore().deleteClaimsForPlayer(player.getUniqueId(), player, false);

			// inform the player
			final int remainingBlocks = playerData.getRemainingClaimBlocks();
			GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.SuccessfulAbandon, String.valueOf(remainingBlocks));

			// revert any current visualization
			Visualization.Revert(player);

			return true;
		}

		// restore nature
		else if (cmd.getName().equalsIgnoreCase("restorenature") && (player != null)) {
			// change shovel mode
			final PlayerData playerData = this.gpp.getDataStore().getPlayerData(player.getUniqueId());
			playerData.shovelMode = ShovelMode.RestoreNature;
			GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.RestoreNatureActivate);
			return true;
		}

		// restore nature aggressive mode
		else if (cmd.getName().equalsIgnoreCase("restorenatureaggressive") && (player != null)) {
			// change shovel mode
			final PlayerData playerData = this.gpp.getDataStore().getPlayerData(player.getUniqueId());
			playerData.shovelMode = ShovelMode.RestoreNatureAggressive;
			GriefPreventionPlus.sendMessage(player, TextMode.Warn, Messages.RestoreNatureAggressiveActivate);
			return true;
		}

		// restore nature fill mode
		else if (cmd.getName().equalsIgnoreCase("restorenaturefill") && (player != null)) {
			// change shovel mode
			final PlayerData playerData = this.gpp.getDataStore().getPlayerData(player.getUniqueId());
			playerData.shovelMode = ShovelMode.RestoreNatureFill;

			// set radius based on arguments
			playerData.fillRadius = 2;
			if (args.length > 0) {
				try {
					playerData.fillRadius = Integer.parseInt(args[0]);
				} catch (final Exception exception) {
				}
			}

			if (playerData.fillRadius < 0) {
				playerData.fillRadius = 2;
			}

			GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.FillModeActive, String.valueOf(playerData.fillRadius));
			return true;
		}

		// trust <player>
		else if (cmd.getName().equalsIgnoreCase("trust") && (player != null)) {
			// requires exactly one parameter, the other player's name
			if (args.length != 1) {
				return false;
			}

			// most trust commands use this helper method, it keeps them
			// consistent
			this.handleTrustCommand(player, ClaimPermission.BUILD, args[0]);

			return true;
		}

		// transferclaim <player>
		else if (cmd.getName().equalsIgnoreCase("transferclaim") && (player != null)) {
			// which claim is the user in?
			final Claim claim = this.gpp.getDataStore().getClaimAt(player.getLocation(), true);
			if (claim == null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.TransferClaimMissing);
				return true;
			}

			// check additional permission for admin claims
			if (claim.isAdminClaim() && !player.hasPermission("griefprevention.adminclaims")) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.TransferClaimPermission);
				return true;
			}

			UUID newOwnerID = GriefPreventionPlus.UUID1;
			String ownerName = "admin";

			if (args.length > 0) {
				final OfflinePlayer targetPlayer = GriefPreventionPlus.getInstance().resolvePlayer(args[0]);
				if (targetPlayer == null) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.PlayerNotFound2);
					return true;
				}
				newOwnerID = targetPlayer.getUniqueId();
				ownerName = targetPlayer.getName();
			}
			
			// call event
			ClaimOwnerTransfer event = new ClaimOwnerTransfer(claim, player, newOwnerID);
			Bukkit.getPluginManager().callEvent(event);
			if (event.isCancelled()) {
				return true;
			}

			// change ownerhsip
			try {
				this.gpp.getDataStore().changeClaimOwner(claim, newOwnerID);
			} catch (final Exception e) {
				e.printStackTrace();
				GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.TransferTopLevel);
				return true;
			}

			// confirm
			GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.TransferSuccess);
			GriefPreventionPlus.addLogEntry(player.getName() + " transferred a claim at " + GriefPreventionPlus.getfriendlyLocationString(claim.getLesserBoundaryCorner()) + " to " + ownerName + ".");

			return true;
		}

		// trustlist
		else if (cmd.getName().equalsIgnoreCase("trustlist") && (player != null)) {
			final Claim claim;
			if (args.length==0) {
				claim = this.gpp.getDataStore().getClaimAt(player.getLocation(), true);
				
				// if no claim here, error message
				if (claim == null) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.TrustListNoClaim);
					return true;
				}

			} else {
				try {
					int claimId = Integer.valueOf(args[0]);
					claim = this.gpp.getDataStore().getClaim(claimId);
					
					if (claim==null) {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, "No claim found with the specified ID."); // move to messages
						return true;
					}
				} catch (NumberFormatException e) {
					return false;
				}
			}


			if (!player.hasPermission("griefprevention.trustlist")){
				// if no permission to manage permissions, error message
				final String errorMessage = claim.canGrantPermission(player);
				if (errorMessage != null) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, errorMessage);
					return true;
				}
			}

			// otherwise build a list of explicit permissions by permission level and send that to the player
			player.sendMessage(claim.getTrustList());
			return true;
		}

		// untrust <player> or untrust [<group>]
		else if (cmd.getName().equalsIgnoreCase("untrust") && (player != null)) {
			// requires exactly one parameter, the other player's name
			if (args.length != 1) {
				return false;
			}

			String permBukkit = null;
			// if a permissionBukkit
			if (args[0].startsWith("[") && args[0].endsWith("]")) {
				permBukkit = args[0].substring(1, args[0].length() - 1);
			} else if (args[0].startsWith("#")) {
				permBukkit = args[0];
			}

			// determine which claim the player is standing in
			final Claim claim = this.gpp.getDataStore().getClaimAt(player.getLocation(), true /*
																							 * ignore
																							 * height
																							 */);

			if (claim == null) { // all player's claims
				if (this.gpp.getDataStore().getPlayerData(player.getUniqueId()).getClaims().size() == 0) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.YouHaveNoClaims);
					return false;
				}

				if (args[0].equals("all")) { // clear all permissions from
					// player's claims
					this.gpp.getDataStore().clearPermissionsOnPlayerClaims(player.getUniqueId());

					GriefPreventionPlus.addLogEntry(player.getName() + " removed all permissions from his claims");

					GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.UntrustEveryoneAllClaims);
				} else {// remove specific permission from player's claims
					if (permBukkit != null) { // permissionbukkit
						if (args[0].length() < 3) {
							GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.InvalidPermissionID);
							return false;
						}
						this.gpp.getDataStore().dropPermissionOnPlayerClaims(player.getUniqueId(), permBukkit);
						GriefPreventionPlus.addLogEntry(player.getName() + " removed " + args[0] + " permission from his claims");

					} else if (args[0].equals("public")) { // public
						this.gpp.getDataStore().dropPermissionOnPlayerClaims(player.getUniqueId(), GriefPreventionPlus.UUID0);
						GriefPreventionPlus.addLogEntry(player.getName() + " removed public permission from his claims");
					} else { // player?
						final OfflinePlayer otherPlayer = this.gpp.resolvePlayer(args[0]);
						if (otherPlayer == null) {// player not found
							GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.PlayerNotFound2);
							return true;
						}
						this.gpp.getDataStore().dropPermissionOnPlayerClaims(player.getUniqueId(), otherPlayer.getUniqueId());
						GriefPreventionPlus.addLogEntry(player.getName() + " removed " + otherPlayer.getName() + " permission from his claims");
					}
					GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.UntrustIndividualAllClaims, args[0]);
				}
			} else { // player's standing claim setting
				if (args[0].equals("all")) { // clear claim's perms
					if (claim.canEdit(player) != null) { // no permissions
						GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.ClearPermsOwnerOnly);
						return true;
					}
					this.gpp.getDataStore().dbUnsetPerm(claim.id);
					claim.clearMemoryPermissions();

					GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.UntrustOwnerOnly, claim.getOwnerName());
					GriefPreventionPlus.addLogEntry(player.getName() + " removed all permissions from claim id " + claim.id);
				} else {
					if (claim.canGrantPermission(player) != null) {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NoPermissionTrust, claim.getOwnerName());
						return true;
					}

					if (permBukkit != null) { // permissionbukkit
						if (args[0].length() < 3) {
							GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.InvalidPermissionID);
							return false;
						}

						// a manager needs the same permission or higher
						if (claim.checkPermission(player, claim.getPermission(permBukkit)) != null) {
							GriefPreventionPlus.sendMessage(player, TextMode.Err, "You need an higher permission to remove this permission.");
							return false;
						}

						this.gpp.getDataStore().dbUnsetPerm(claim.id, permBukkit);
						claim.unsetPermission(permBukkit);
						GriefPreventionPlus.addLogEntry(player.getName() + " removed " + args[0] + " permission from claim id " + claim.id);
					} else if (args[0].equals("public")) { // public
						this.gpp.getDataStore().dbUnsetPerm(claim.id, GriefPreventionPlus.UUID0);
						claim.unsetPermission(GriefPreventionPlus.UUID0);
						GriefPreventionPlus.addLogEntry(player.getName() + " removed public permission from claim id " + claim.id);
					} else { // player?
						final OfflinePlayer otherPlayer = this.gpp.resolvePlayer(args[0]);
						if (otherPlayer == null) {// player not found
							GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.PlayerNotFound2);
							return true;
						}

						// a manager needs the same permission or higher
						if (claim.checkPermission(player, claim.getPermission(otherPlayer.getUniqueId())) != null) {
							GriefPreventionPlus.sendMessage(player, TextMode.Err, "You need an higher permission to remove this permission.");
							return false;
						}

						this.gpp.getDataStore().dbUnsetPerm(claim.id, otherPlayer.getUniqueId());
						claim.unsetPermission(otherPlayer.getUniqueId());
						GriefPreventionPlus.addLogEntry(player.getName() + " removed " + otherPlayer.getName() + " permission from claim id " + claim.id);
					}
					GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.UntrustIndividualSingleClaim, args[0]);
				}
			}

			return true;
		}
		
		// entrytrust <player>
		else if (cmd.getName().equalsIgnoreCase("entrytrust") && (player != null)) {
			// requires exactly one parameter, the other player's name
			if (args.length != 1) {
				return false;
			}

			this.handleTrustCommand(player, ClaimPermission.ENTRY, args[0]);

			return true;
		}

		// accesstrust <player>
		else if (cmd.getName().equalsIgnoreCase("accesstrust") && (player != null)) {
			// requires exactly one parameter, the other player's name
			if (args.length != 1) {
				return false;
			}

			this.handleTrustCommand(player, ClaimPermission.ACCESS, args[0]);

			return true;
		}

		// containertrust <player>
		else if (cmd.getName().equalsIgnoreCase("containertrust") && (player != null)) {
			// requires exactly one parameter, the other player's name
			if (args.length != 1) {
				return false;
			}

			this.handleTrustCommand(player, ClaimPermission.CONTAINER, args[0]);

			return true;
		}

		// permissiontrust <player>
		else if (cmd.getName().equalsIgnoreCase("permissiontrust") && (player != null)) {
			// requires exactly one parameter, the other player's name
			if (args.length != 1) {
				return false;
			}

			this.handleTrustCommand(player, ClaimPermission.MANAGE, args[0]);

			return true;
		}

		// buyclaimblocks
		else if (cmd.getName().equalsIgnoreCase("buyclaimblocks") && (player != null)) {
			// if economy is disabled, don't do anything
			if (GriefPreventionPlus.economy == null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.BuySellNotConfigured);
				return true;
			}

			if (!player.hasPermission("griefprevention.buysellclaimblocks")) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NoPermissionForCommand);
				return true;
			}

			// if purchase disabled, send error message
			if (GriefPreventionPlus.getInstance().config.economy_claimBlocksPurchaseCost == 0) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.OnlySellBlocks);
				return true;
			}

			// if no parameter, just tell player cost per block and balance
			if (args.length != 1) {
				GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.BlockPurchaseCost, String.valueOf(GriefPreventionPlus.getInstance().config.economy_claimBlocksPurchaseCost), String.valueOf(GriefPreventionPlus.economy.getBalance(player)));
				return false;
			}

			else {
				final PlayerData playerData = this.gpp.getDataStore().getPlayerData(player.getUniqueId());

				// try to parse number of blocks
				int blockCount;
				try {
					blockCount = Integer.parseInt(args[0]);
				} catch (final NumberFormatException numberFormatException) {
					return false; // causes usage to be displayed
				}

				if (blockCount <= 0) {
					return false;
				}

				// if the player can't afford his purchase, send error message
				final double balance = GriefPreventionPlus.economy.getBalance(player);
				final double totalCost = blockCount * GriefPreventionPlus.getInstance().config.economy_claimBlocksPurchaseCost;
				if (totalCost > balance) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.InsufficientFunds, String.valueOf(totalCost), String.valueOf(balance));
				}

				// otherwise carry out transaction
				else {
					// withdraw cost
					GriefPreventionPlus.economy.withdrawPlayer(player, totalCost);

					// add blocks
					playerData.setBonusClaimBlocks(playerData.getBonusClaimBlocks() + blockCount);
					this.gpp.getDataStore().savePlayerData(player.getUniqueId(), playerData);

					// inform player
					GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.PurchaseConfirmation, String.valueOf(totalCost), String.valueOf(playerData.getRemainingClaimBlocks()));
				}

				return true;
			}
		}

		// sellclaimblocks <amount>
		else if (cmd.getName().equalsIgnoreCase("sellclaimblocks") && (player != null)) {
			// if economy is disabled, don't do anything
			if (GriefPreventionPlus.economy == null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.BuySellNotConfigured);
				return true;
			}

			if (!player.hasPermission("griefprevention.buysellclaimblocks")) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NoPermissionForCommand);
				return true;
			}

			// if disabled, error message
			if (GriefPreventionPlus.getInstance().config.economy_claimBlocksSellValue == 0) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.OnlyPurchaseBlocks);
				return true;
			}

			// load player data
			final PlayerData playerData = this.gpp.getDataStore().getPlayerData(player.getUniqueId());
			int availableBlocks = playerData.getBonusClaimBlocks();
			for (Claim claim : playerData.getClaims()) {
				availableBlocks-=claim.getArea();
			}
			
			if (availableBlocks<0) {
				availableBlocks+=playerData.getAccruedClaimBlocks();
			}

			// if no amount provided, just tell player value per block sold, and
			// how many he can sell
			if (args.length != 1) {
				GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.BlockSaleValue, String.valueOf(GriefPreventionPlus.getInstance().config.economy_claimBlocksSellValue), String.valueOf(availableBlocks));
				return false;
			}

			// parse number of blocks
			int blockCount;
			try {
				blockCount = Integer.parseInt(args[0]);
			} catch (final NumberFormatException numberFormatException) {
				return false; // causes usage to be displayed
			}

			if (blockCount <= 0) {
				return false;
			}

			// if he doesn't have enough blocks, tell him so
			if (blockCount > availableBlocks) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NotEnoughBlocksForSale);
			}

			// otherwise carry out the transaction
			else {
				// compute value and deposit it
				final double totalValue = blockCount * GriefPreventionPlus.getInstance().config.economy_claimBlocksSellValue;
				GriefPreventionPlus.economy.depositPlayer(player, totalValue);

				// subtract blocks
				playerData.setBonusClaimBlocks(playerData.getBonusClaimBlocks() - blockCount);
				this.gpp.getDataStore().savePlayerData(player.getUniqueId(), playerData);

				// inform player
				GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.BlockSaleConfirmation, String.valueOf(totalValue), String.valueOf(playerData.getRemainingClaimBlocks()));
			}

			return true;
		}

		// adminclaims
		else if (cmd.getName().equalsIgnoreCase("adminclaims") && (player != null)) {
			final PlayerData playerData = this.gpp.getDataStore().getPlayerData(player.getUniqueId());
			playerData.shovelMode = ShovelMode.Admin;
			GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.AdminClaimsMode);

			return true;
		}

		// basicclaims
		else if (cmd.getName().equalsIgnoreCase("basicclaims") && (player != null)) {
			final PlayerData playerData = this.gpp.getDataStore().getPlayerData(player.getUniqueId());
			playerData.shovelMode = ShovelMode.Basic;
			playerData.claimSubdividing = null;
			GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.BasicClaimsMode);

			return true;
		}

		// subdivideclaims
		else if (cmd.getName().equalsIgnoreCase("subdivideclaims") && (player != null)) {
			final PlayerData playerData = this.gpp.getDataStore().getPlayerData(player.getUniqueId());
			playerData.shovelMode = ShovelMode.Subdivide;
			playerData.claimSubdividing = null;
			GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.SubdivisionMode);
			GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.SubdivisionVideo2, DataStore.SUBDIVISION_VIDEO_URL);

			return true;
		}

		// deleteclaim
		else if (cmd.getName().equalsIgnoreCase("deleteclaim") && (player != null)) {
			Claim claim;
			// determine which claim the player is standing in
			if (args.length == 0) {
				claim = this.gpp.getDataStore().getClaimAt(player.getLocation());
			} else { // GPP's feature: delete a claim by ID
				try {
					claim = this.gpp.getDataStore().getClaim(Integer.valueOf(args[0]));
				} catch (final NumberFormatException e) {
					player.sendMessage("Invalid ID");
					return false;
				}
			}
			
			if (claim == null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.DeleteClaimMissing);
				return false;
			}

			// deleting an admin claim additionally requires the adminclaims
			// permission
			if (!claim.isAdminClaim() || player.hasPermission("griefprevention.adminclaims")) {
				final PlayerData playerData = this.gpp.getDataStore().getPlayerData(player.getUniqueId());
				if ((claim.getChildren().size() > 0) && !playerData.warnedAboutMajorDeletion) {
					GriefPreventionPlus.sendMessage(player, TextMode.Warn, Messages.DeletionSubdivisionWarning);
					playerData.warnedAboutMajorDeletion = true;
				} else {
					// fire event
					final ClaimDeleteEvent event = new ClaimDeleteEvent(claim, player, Reason.DELETE);
					GriefPreventionPlus.getInstance().getServer().getPluginManager().callEvent(event);
					if (event.isCancelled()) {
						return false;
					}

					claim.removeSurfaceFluids(null);
					this.gpp.getDataStore().deleteClaim(claim);

					// if in a creative mode world, /restorenature the claim
					if (GriefPreventionPlus.getInstance().creativeRulesApply(claim.getWorld())) {
						GriefPreventionPlus.getInstance().restoreClaim(claim, 0);
					}

					GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.DeleteSuccess);
					GriefPreventionPlus.addLogEntry(player.getName() + " deleted " + claim.getOwnerName() + "'s claim at " + GriefPreventionPlus.getfriendlyLocationString(claim.getLesserBoundaryCorner()));

					// revert any current visualization
					Visualization.Revert(player);

					playerData.warnedAboutMajorDeletion = false;
				}
			} else {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.CantDeleteAdminClaim);
			}

			return true;
		}

		else if (cmd.getName().equalsIgnoreCase("claimexplosions") && (player != null)) {
			// determine which claim the player is standing in
			final Claim claim = this.gpp.getDataStore().getClaimAt(player.getLocation(), true /*
																							 * ignore
																							 * height
																							 */);

			if (claim == null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.DeleteClaimMissing);
			}

			else {
				final String noBuildReason = claim.canBuild(player, Material.TNT);
				if (noBuildReason != null) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, noBuildReason);
					return true;
				}

				if (claim.areExplosivesAllowed()) {
					claim.setExplosivesAllowed(false);
					GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.ExplosivesDisabled);
				} else {
					claim.setExplosivesAllowed(true);
					GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.ExplosivesEnabled);
				}
			}

			return true;
		}

		// deleteallclaims <player>
		else if (cmd.getName().equalsIgnoreCase("deleteallclaims")) {
			// requires exactly one parameter, the other player's name
			if (args.length != 1) {
				return false;
			}

			// try to find that player
			final OfflinePlayer otherPlayer = this.gpp.resolvePlayer(args[0]);
			if (otherPlayer == null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.PlayerNotFound2);
				return true;
			}

			// delete all that player's claims
			this.gpp.getDataStore().deleteClaimsForPlayer(otherPlayer.getUniqueId(), player, true);

			GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.DeleteAllSuccess, otherPlayer.getName());
			if (player != null) {
				GriefPreventionPlus.addLogEntry(player.getName() + " deleted all claims belonging to " + otherPlayer.getName() + ".");

				// revert any current visualization
				Visualization.Revert(player);
			}

			return true;
		}

		// claimslist or claimslist <player>
		else if (cmd.getName().equalsIgnoreCase("claimslist")) {
			// at most one parameter
			if (args.length > 1) {
				return false;
			}

			// player whose claims will be listed
			OfflinePlayer otherPlayer;

			// if another player isn't specified, assume current player
			if (args.length < 1) {
				if (player != null) {
					otherPlayer = player;
				} else {
					return false;
				}
			}

			// otherwise if no permission to delve into another player's claims
			// data
			else if ((player != null) && !player.hasPermission("griefprevention.claimslistother")) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.ClaimsListNoPermission);
				return true;
			}

			// otherwise try to find the specified player
			else {
				otherPlayer = this.gpp.resolvePlayer(args[0]);
				if (otherPlayer == null) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.PlayerNotFound2);
					return true;
				}
			}

			// load the target player's data
			final PlayerData playerData = this.gpp.getDataStore().getPlayerData(otherPlayer.getUniqueId());
			GriefPreventionPlus.sendMessage(player, TextMode.Instr, " " + playerData.getAccruedClaimBlocks() + " blocks from play +" + (playerData.getBonusClaimBlocks() + this.gpp.getDataStore().getGroupBonusBlocks(otherPlayer.getUniqueId())) + " bonus = " + (playerData.getAccruedClaimBlocks() + playerData.getBonusClaimBlocks() + this.gpp.getDataStore().getGroupBonusBlocks(otherPlayer.getUniqueId())) + " total.");
			GriefPreventionPlus.sendMessage(player, TextMode.Instr, "Your Claims:");
			if (playerData.getClaims().size() > 0) {
				for (int i = 0; i < playerData.getClaims().size(); i++) {
					final Claim claim = playerData.getClaims().get(i);
					GriefPreventionPlus.sendMessage(player, TextMode.Instr, "ID: " + claim.id + " " + GriefPreventionPlus.getfriendlyLocationString(claim.getLesserBoundaryCorner()) + " (-" + claim.getArea() + " blocks)");
				}

				GriefPreventionPlus.sendMessage(player, TextMode.Instr, " = " + playerData.getRemainingClaimBlocks() + " blocks left to spend");
			}

			return true;
		}


		// deletealladminclaims
		else if ((player != null) && cmd.getName().equalsIgnoreCase("deletealladminclaims")) {
			if (!player.hasPermission("griefprevention.deleteclaims")) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NoDeletePermission);
				return true;
			}

			// delete all admin claims
			this.gpp.getDataStore().deleteClaimsForPlayer(null, player, true);

			GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.AllAdminDeleted);
			if (player != null) {
				GriefPreventionPlus.addLogEntry(player.getName() + " deleted all administrative claims.");

				// revert any current visualization
				Visualization.Revert(player);
			}

			return true;
		}

		// adjustbonusclaimblocks <player> <amount> or [<permission>] amount
		else if (cmd.getName().equalsIgnoreCase("adjustbonusclaimblocks")) {
			// requires exactly two parameters, the other player or group's name
			// and the adjustment
			if (args.length != 2) {
				return false;
			}

			// parse the adjustment amount
			int adjustment;
			try {
				adjustment = Integer.parseInt(args[1]);
			} catch (final NumberFormatException numberFormatException) {
				return false; // causes usage to be displayed
			}

			// if granting blocks to all players with a specific permission
			if (args[0].startsWith("[") && args[0].endsWith("]")) {
				final String permissionIdentifier = args[0].substring(1, args[0].length() - 1);
				final int newTotal = this.gpp.getDataStore().adjustGroupBonusBlocks(permissionIdentifier, adjustment);

				GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.AdjustGroupBlocksSuccess, permissionIdentifier, String.valueOf(adjustment), String.valueOf(newTotal));
				if (player != null) {
					GriefPreventionPlus.addLogEntry(player.getName() + " adjusted " + permissionIdentifier + "'s bonus claim blocks by " + adjustment + ".");
				}

				return true;
			}

			// otherwise, find the specified player
			final OfflinePlayer targetPlayer = this.gpp.resolvePlayer(args[0]);
			if (targetPlayer == null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.PlayerNotFound2);
				return true;
			}

			// give blocks to player
			final PlayerData playerData = this.gpp.getDataStore().getPlayerData(targetPlayer.getUniqueId());
			playerData.setBonusClaimBlocks(playerData.getBonusClaimBlocks() + adjustment);
			this.gpp.getDataStore().savePlayerData(targetPlayer.getUniqueId(), playerData);

			GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.AdjustBlocksSuccess, targetPlayer.getName(), String.valueOf(adjustment), String.valueOf(playerData.getBonusClaimBlocks()));
			if (player != null) {
				GriefPreventionPlus.addLogEntry(player.getName() + " adjusted " + targetPlayer.getName() + "'s bonus claim blocks by " + adjustment + ".");
			}

			return true;
		}
		

		else if(cmd.getName().equalsIgnoreCase("adjustbonusclaimblocksall"))
		   {
		       //requires exactly one parameter, the amount of adjustment
		       if(args.length != 1) return false;
		       
		       //parse the adjustment amount
		       int adjustment;         
		       try
		       {
		           adjustment = Integer.parseInt(args[0]);
		       }
		       catch(NumberFormatException numberFormatException)
		       {
		           return false;  //causes usage to be displayed
		       }
		       
		       //for each online player
		       @SuppressWarnings("unchecked")
		       Collection<Player> players = (Collection<Player>) gpp.getServer().getOnlinePlayers();
		       for(Player onlinePlayer : players)
		       {
		           UUID playerID = onlinePlayer.getUniqueId();
		           PlayerData playerData = this.dataStore.getPlayerData(playerID);
		           playerData.setBonusClaimBlocks(playerData.getBonusClaimBlocks() + adjustment);
		           this.dataStore.savePlayerData(playerID, playerData);
		       }
		       
		       GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.AdjustBlocksAllSuccess, String.valueOf(adjustment));
		       if(player != null) GriefPreventionPlus.addLogEntry(player.getName() + " adjusted all players' bonus claim blocks by " + adjustment + ".");
		       
		       return true;
		   }
		// setaccruedclaimblocks <player> <amount>
		else if (cmd.getName().equalsIgnoreCase("setaccruedclaimblocks")) {
			// requires exactly two parameters, the other player's name and the
			// new amount
			if (args.length != 2) {
				return false;
			}

			// parse the adjustment amount
			int newAmount;
			try {
				newAmount = Integer.parseInt(args[1]);
			} catch (final NumberFormatException numberFormatException) {
				return false; // causes usage to be displayed
			}

			// find the specified player
			final OfflinePlayer targetPlayer = GriefPreventionPlus.getInstance().resolvePlayer(args[0]);
			if (targetPlayer == null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.PlayerNotFound2);
				return true;
			}

			// set player's blocks
			final PlayerData playerData = this.dataStore.getPlayerData(targetPlayer.getUniqueId());
			playerData.setAccruedClaimBlocks(newAmount);
			this.dataStore.savePlayerData(targetPlayer.getUniqueId(), playerData);

			GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.SetClaimBlocksSuccess);
			if (player != null) {
				GriefPreventionPlus.addLogEntry(player.getName() + " set " + targetPlayer.getName() + "'s accrued claim blocks to " + newAmount + ".");
			}

			return true;
		}
		// trapped
		else if (cmd.getName().equalsIgnoreCase("trapped") && (player != null)) {
			// FEATURE: empower players who get "stuck" in an area where they
			// don't have permission to build to save themselves

			final PlayerData playerData = this.gpp.getDataStore().getPlayerData(player.getUniqueId());
			final Claim claim = this.gpp.getDataStore().getClaimAt(player.getLocation(), false, playerData.lastClaim);

			// if another /trapped is pending, ignore this slash command
			if (playerData.pendingTrapped) {
				return true;
			}

			// if the player isn't in a claim or has permission to build, tell
			// him to man up
			if ((claim == null) || (claim.canBuild(player, Material.AIR) == null)) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NotTrappedHere);
				return true;
			}

			// if the player is in the nether or end, he's screwed (there's no
			// way to programmatically find a safe place for him)
			if (player.getWorld().getEnvironment() != Environment.NORMAL) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.TrappedWontWorkHere);
				return true;
			}

			// if the player is in an administrative claim, he should contact an
			// admin
			if (claim.isAdminClaim()) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.TrappedWontWorkHere);
				return true;
			}

			// send instructions
			GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.RescuePending);

			// create a task to rescue this player in a little while
			final PlayerRescueTask task = new PlayerRescueTask(player, player.getLocation());
			this.gpp.getServer().getScheduler().scheduleSyncDelayedTask(this.gpp, task, 200L); // 20L
																								// ~
																								// 1
																								// second

			return true;
		}

		else if (cmd.getName().equalsIgnoreCase("gpreload")) {
			this.gpp.config = new Config();
			if (player != null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Success, "Configuration updated.  If you have updated your Grief Prevention JAR, you still need to /reload or reboot your server.");
			} else {
				GriefPreventionPlus.addLogEntry("Configuration updated.  If you have updated your Grief Prevention JAR, you still need to /reload or reboot your server.");
			}

			return true;
		}

		// givepet
		else if (cmd.getName().equalsIgnoreCase("givepet") && (player != null)) {
			// requires one parameter
			if (args.length < 1) {
				return false;
			}

			final PlayerData playerData = this.gpp.getDataStore().getPlayerData(player.getUniqueId());

			// special case: cancellation
			if (args[0].equalsIgnoreCase("cancel")) {
				playerData.petGiveawayRecipient = null;
				GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.PetTransferCancellation);
				return true;
			}

			// find the specified player
			final OfflinePlayer targetPlayer = this.gpp.resolvePlayer(args[0]);
			if (targetPlayer == null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.PlayerNotFound2);
				return true;
			}

			// remember the player's ID for later pet transfer
			playerData.petGiveawayRecipient = targetPlayer;

			// send instructions
			GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.ReadyToTransferPet);

			return true;
		}

		// gpblockinfo
		else if (cmd.getName().equalsIgnoreCase("gpblockinfo") && (player != null)) {
			final ItemStack inHand = player.getItemInHand();
			player.sendMessage("In Hand: " + String.format("%s(%d:%d)", inHand.getType().name(), inHand.getTypeId(), inHand.getData().getData()));

			final Block inWorld = GriefPreventionPlus.getTargetNonAirBlock(player, 300);
			player.sendMessage("In World: " + String.format("%s(%d:%d)", inWorld.getType().name(), inWorld.getTypeId(), inWorld.getData()));

			return true;
		}
		// claim area
		else if (cmd.getName().equalsIgnoreCase("claimarea") && (sender.hasPermission("griefprevention.claimarea"))) {
			if (args.length != 4) {
				return false;
			}
			try {
				final World world=GriefPreventionPlus.getInstance().getServer().getWorld(args[0]);
				if (world==null) {
					sender.sendMessage("Invalid world: "+world);
					return false;
				}
				final int x = Integer.valueOf(args[1]);
				final int z = Integer.valueOf(args[2]);
				
				final int range = Integer.valueOf(args[3]);
				final int side = (range * 2) + 1;
				if (side < GriefPreventionPlus.getInstance().config.claims_minSize) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NewClaimTooSmall, String.valueOf(GriefPreventionPlus.getInstance().config.claims_minSize));
					return true;
				}
				
				if (player!=null) {
					final int newClaimArea = side * side;
					final PlayerData playerData = this.dataStore.getPlayerData(player.getUniqueId());
					final int remainingBlocks = playerData.getRemainingClaimBlocks();
					if (newClaimArea > remainingBlocks) {
						GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.CreateClaimInsufficientBlocks, String.valueOf(newClaimArea - remainingBlocks));
						PlayerEventHandler.tryAdvertiseAdminAlternatives(player);
						return true;
					}
				}
				final int x1 = x - range, x2 = x + range, z1 = z - range, z2 = z + range;
				
				// try to create a new claim
				final ClaimResult result = this.dataStore.newClaim(world.getUID(), x1, z1, x2, z2, (player!=null ? player.getUniqueId() : GriefPreventionPlus.UUID1), null, null, player);
				if (player==null && result.getResult()!=Result.SUCCESS) {
					sender.sendMessage("Your selected area overlaps an existing claim.");
				} else {
					switch(result.getResult()) {
						case EVENT:{
							// show the message set by the event
							if (result.getReason()!=null) {
								GriefPreventionPlus.sendMessage(player, TextMode.Err, result.getReason());
							}
							break;
						}
						case OVERLAP:{
							GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.CreateClaimFailOverlapShort);
							
							final Visualization visualization = Visualization.FromClaim(result.getClaim(), player.getLocation().getBlockY(), VisualizationType.ErrorClaim, player.getLocation());
							Visualization.Apply(player, visualization);
							break;
						}
						case SUCCESS:{
							GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.CreateClaimSuccess);
							if (player!=null) {
								final Visualization visualization = Visualization.FromClaim(result.getClaim(), player.getLocation().getBlockY(), VisualizationType.Claim, player.getLocation());
								Visualization.Apply(player, visualization);
			
								// if it's a big claim, tell the player about subdivisions
								if (!player.hasPermission("griefprevention.adminclaims") && (result.getClaim().getArea() >= 1000)) {
									GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.BecomeMayor, 200L);
									GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.SubdivisionVideo2, 201L, DataStore.SUBDIVISION_VIDEO_URL);
								}
							}
							break;
						}
						case WGREGION:{
							GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.CreateClaimFailOverlapRegion);
							break;
						}
					}
				}
			} catch (final NumberFormatException e) {
				return false;
			}

			return true;
		}
		// convertdatabase
		else if (cmd.getName().equalsIgnoreCase("gpp") && (sender.hasPermission("griefprevention.gpp"))) {
			if (args.length == 0 || !args[0].equalsIgnoreCase("convertdatabase")){
				sender.sendMessage( "§a/" + commandLabel + " convertdatabase");
				return true;
			}

			if ( !(GriefPreventionPlus.getInstance().getDataStore() instanceof DataStoreMySQL)){
				sender.sendMessage("You can only convert from MySQL to YML!");
				return true;
			}

			if (args.length < 2 || !args[1].equalsIgnoreCase("--confirm")){
				sender.sendMessage( "To confirm this operation, use §a/" + commandLabel + " convertdatabase --confirm");
				return true;
			}

			if (exporting){
				sender.sendMessage("§cAlready exporting, please wait!");
				return true;
			}

			exporting = true;
			new Thread(){
				@Override
				public void run() {
					try {
						DataStoreMySQL dataStoreMySQL = (DataStoreMySQL) GriefPreventionPlus.getInstance().getDataStore();

						sender.sendMessage("§7Loading entire PlayerData from MYSQL");
						List<PlayerData> playerDataList = dataStoreMySQL.getEntirePlayerDataFromDatabase();

						sender.sendMessage("§7Creating DataStoreYML...");
						File gppDataFolder = new File(new File(DataStore.configFilePath).getParent(), "DataStorageYML");
						gppDataFolder.delete();
						DataStoreYML dataStorYML = new DataStoreYML();

						sender.sendMessage("§7Converting PlayerDatas...");
						for (PlayerData playerData : playerDataList) {
							try {
								dataStorYML.asyncSavePlayerData(playerData.playerID, playerData);
								sender.sendMessage("PlayerData [" + playerData.playerID + "] was converted.");
							}catch (Exception e){
								sender.sendMessage("Failed to add PlayerData [" + playerData.playerID + "]");
								e.printStackTrace();
							}
						}

						sender.sendMessage("§7Converting Claims...");
						for (Claim claim : dataStoreMySQL.claims.values()) {
							try {
								dataStorYML.dbNewClaimWithID(claim);
								int perms = 0;
								for (Map.Entry<UUID, Integer> uuidIntegerEntry : claim.getPermissionMapPlayers().entrySet()) {
									dataStorYML.dbSetPerm(claim.getID(), uuidIntegerEntry.getKey(), uuidIntegerEntry.getValue());
									perms++;
								}
								for (Map.Entry<String, Integer> stringIntegerEntry : claim.getPermissionMapBukkit().entrySet()) {
									dataStorYML.dbSetPerm(claim.getID(), stringIntegerEntry.getKey(), stringIntegerEntry.getValue());
									perms++;
								}
								for (Map.Entry<String, Integer> stringIntegerEntry : claim.getPermissionMapFakePlayers().entrySet()) {
									dataStorYML.dbSetPerm(claim.getID(), "#" + stringIntegerEntry.getKey(), stringIntegerEntry.getValue());
									perms++;
								}
								sender.sendMessage("Claim [" + claim.getID() + "] was converted with " + perms + " trusts.");
							}catch (Exception e){
								sender.sendMessage("Failed to add claim [" + claim.getID() + "] from" + claim.getOwnerName());
								e.printStackTrace();
							}
						}

						sender.sendMessage("§aConversion Done!");
					}catch (Exception e){
						sender.sendMessage("Failed to create the DataStoreYML");
						e.printStackTrace();
					}
					exporting = false;
				}
			}.start();
		}
		return false;
	}

	boolean exporting = false;
	
	boolean abandonClaimHandler(Player player, boolean deleteTopLevelClaim) {
		return abandonClaimHandler(player, deleteTopLevelClaim, -1);
	}

	boolean abandonClaimHandler(Player player, boolean deleteTopLevelClaim, int claimId) {
		final PlayerData playerData = this.dataStore.getPlayerData(player.getUniqueId());
		
		// which claim is being abandoned?
		final Claim claim;
		if (claimId==-1) {
			claim = this.dataStore.getClaimAt(player.getLocation(), true);
		} else {
			claim = this.dataStore.getClaim(claimId);
		}
		
		if (claim == null) {
			GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.AbandonClaimMissing);
		}

		// verify ownership
		else if (claim.canEdit(player) != null) {
			GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NotYourClaim);
		}

		// warn if has children and we're not explicitly deleting a top level
		// claim
		else if ((claim.getChildren().size() > 0) && !deleteTopLevelClaim) {
			GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.DeleteTopLevelClaim);
			return true;
		}

		else {
			// fire event
			final ClaimDeleteEvent event = new ClaimDeleteEvent(claim, player, Reason.ABANDON);
			GriefPreventionPlus.getInstance().getServer().getPluginManager().callEvent(event);
			if (event.isCancelled()) {
				return true;
			}

			// delete it
			claim.removeSurfaceFluids(null);
			GriefPreventionPlus.addLogEntry(player.getName() + " deleted claim id " + claim.id + " at " + claim.locationToString());
			this.dataStore.deleteClaim(claim);

			// if in a creative mode world, restore the claim area
			if (GriefPreventionPlus.getInstance().creativeRulesApply(claim.getWorld())) {
				// GriefPreventionPlus.AddLogEntry(player.getName() +
				// " abandoned a claim @ " +
				// GriefPreventionPlus.getfriendlyLocationString(claim.getLesserBoundaryCorner()));
				GriefPreventionPlus.sendMessage(player, TextMode.Warn, Messages.UnclaimCleanupWarning);
				GriefPreventionPlus.getInstance().restoreClaim(claim, 20L * 60 * 2);
			}

			// adjust claim blocks when abandoning a top level claim
			if (claim.getParent() == null) {
				playerData.setAccruedClaimBlocks(playerData.getAccruedClaimBlocks() - (int) Math.ceil((claim.getArea() * (1 - this.gpp.config.claims_abandonReturnRatio))));
			}

			// tell the player how many claim blocks he has left
			final int remainingBlocks = playerData.getRemainingClaimBlocks();
			GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.AbandonSuccess, String.valueOf(remainingBlocks));

			// revert any current visualization
			Visualization.Revert(player);

			playerData.warnedAboutMajorDeletion = false;
		}

		return true;

	}

	// helper method keeps the trust commands consistent and eliminates
	// duplicate code
	void handleTrustCommand(Player player, ClaimPermission permissionLevel, String recipientName) {
		// determine which claim the player is standing in
		final Claim claim = this.dataStore.getClaimAt(player.getLocation(), true);

		if (claim == null) { // all player's claims
			final PlayerData playerData = this.dataStore.getPlayerData(player.getUniqueId());
			if (playerData.getClaims().size() == 0) {
				// no claims
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.GrantPermissionNoClaim);
				return;
			}

			if (recipientName.startsWith("#") || (recipientName.startsWith("[") && recipientName.endsWith("]"))) { // permissionbukkit
				if (recipientName.length() < 3) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.InvalidPermissionID);
					return;
				}

				for (final Claim c : playerData.getClaims()) {
					c.setPermission(recipientName, permissionLevel);
				}
				GriefPreventionPlus.addLogEntry(player.getName() + " added " + recipientName + " permission (" + (permissionLevel.toString()) + ") to all his claims");
			} else if (recipientName.equals("public")) { // public
				for (final Claim c : playerData.getClaims()) {
					c.setPermission(GriefPreventionPlus.UUID0, permissionLevel);
				}
				GriefPreventionPlus.addLogEntry(player.getName() + " added public permission (" + (permissionLevel.toString()) + ") to all his claims");
			} else { // player?
				final OfflinePlayer otherPlayer = this.gpp.resolvePlayer(recipientName);
				if (otherPlayer == null) {// player not found
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.PlayerNotFound2);
					return;
				}

				for (final Claim c : playerData.getClaims()) {
					c.setPermission(otherPlayer.getUniqueId(), permissionLevel);
				}
				GriefPreventionPlus.addLogEntry(player.getName() + " added " + otherPlayer.getName() + " permission (" + (permissionLevel.toString()) + ") to all his claims");
			}
		} else { // claim the player is standing in
			if (claim.canGrantPermission(player) != null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.NoPermissionTrust, claim.getOwnerName());
				return;
			}

			String errorMessage;
			switch (permissionLevel) {
			case MANAGE:
				errorMessage = claim.canEdit(player);
				if (errorMessage != null) {
					errorMessage = "Only " + claim.getOwnerName() + " can grant /PermissionTrust here.";
				}
				break;
			case ENTRY:
				errorMessage = claim.canEnter(player);
				break;
			case ACCESS:
				errorMessage = claim.canAccess(player);
				break;
			case CONTAINER:
				errorMessage = claim.canOpenContainers(player);
				break;
			default:
				errorMessage = claim.canBuild(player, Material.AIR);
				break;
			}

			if (errorMessage != null) {
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.CantGrantThatPermission);
				return;
			}

			if (recipientName.startsWith("#") || (recipientName.startsWith("[") && recipientName.endsWith("]"))) { // permissionbukkit
				if (recipientName.length() < 3) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.InvalidPermissionID);
					return;
				}
				claim.setPermission(recipientName, permissionLevel);
				GriefPreventionPlus.addLogEntry(player.getName() + " added " + recipientName + " permission (" + (permissionLevel.toString()) + ") to claim id " + claim.id);
			} else if (recipientName.equals("public")) { // public
				claim.setPermission(GriefPreventionPlus.UUID0, permissionLevel);
				GriefPreventionPlus.addLogEntry(player.getName() + " added public permission (" + (permissionLevel.toString()) + ") to claim id " + claim.id);
			} else { // player?
				final OfflinePlayer otherPlayer = this.gpp.resolvePlayer(recipientName);
				if (otherPlayer == null) {// player not found
					GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.PlayerNotFound2);
					return;
				}
				GriefPreventionPlus.addLogEntry(player.getName() + " added " + otherPlayer.getName() + " permission (" + (permissionLevel.toString()) + ") to claim id " + claim.id);
				claim.setPermission(otherPlayer.getUniqueId(), permissionLevel);
			}
		}

		// notify player
		if (recipientName.equals("public")) {
			recipientName = this.dataStore.getMessage(Messages.CollectivePublic);
		}
		String permissionDescription;

		switch (permissionLevel) {
		case MANAGE:
			permissionDescription = this.dataStore.getMessage(Messages.PermissionsPermission);
			break;
		case ENTRY:
			permissionDescription = this.dataStore.getMessage(Messages.EntryPermission);
			break;
		case ACCESS:
			permissionDescription = this.dataStore.getMessage(Messages.AccessPermission);
			break;
		case CONTAINER:
			permissionDescription = this.dataStore.getMessage(Messages.ContainersPermission);
			break;
		default:
			permissionDescription = this.dataStore.getMessage(Messages.BuildPermission);
			break;
		}

		String location;
		if (claim == null) {
			location = this.dataStore.getMessage(Messages.LocationAllClaims);
		} else {
			location = this.dataStore.getMessage(Messages.LocationCurrentClaim);
		}

		GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.GrantPermissionConfirmation, recipientName, permissionDescription, location);
	}

	
	private void createClaim(Player player, PlayerData playerData, int range) {
		final int x = player.getLocation().getBlockX(), z = player.getLocation().getBlockZ(), x1 = x - range, x2 = x + range, z1 = z - range, z2 = z + range;

		// try to create a new claim
		final ClaimResult result = this.dataStore.newClaim(player.getWorld().getUID(), x1, z1, x2, z2, player.getUniqueId(), null, null, player);

		switch(result.getResult()) {
			case EVENT:{
				// show the message set by the event
				if (result.getReason()!=null) {
					GriefPreventionPlus.sendMessage(player, TextMode.Err, result.getReason());
				}
				break;
			}
			case OVERLAP:{
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.CreateClaimFailOverlapShort);

				final Visualization visualization = Visualization.FromClaim(result.getClaim(), player.getLocation().getBlockY(), VisualizationType.ErrorClaim, player.getLocation());
				Visualization.Apply(player, visualization);
				break;
			}
			case SUCCESS:{
				GriefPreventionPlus.sendMessage(player, TextMode.Success, Messages.CreateClaimSuccess);
				final Visualization visualization = Visualization.FromClaim(result.getClaim(), player.getLocation().getBlockY(), VisualizationType.Claim, player.getLocation());
				Visualization.Apply(player, visualization);
				playerData.lastShovelLocation = null;

				// if it's a big claim, tell the player about subdivisions
				if (!player.hasPermission("griefprevention.adminclaims") && (result.getClaim().getArea() >= 1000)) {
					GriefPreventionPlus.sendMessage(player, TextMode.Info, Messages.BecomeMayor, 200L);
					GriefPreventionPlus.sendMessage(player, TextMode.Instr, Messages.SubdivisionVideo2, 201L, DataStore.SUBDIVISION_VIDEO_URL);
				}
				break;
			}
			case WGREGION:{
				GriefPreventionPlus.sendMessage(player, TextMode.Err, Messages.CreateClaimFailOverlapRegion);
				break;
			}
		}
	}
}
